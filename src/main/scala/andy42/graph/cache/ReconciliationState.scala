package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.stream.ZStream
import zio.logging.LogAnnotation

import java.time.temporal.ChronoUnit.MILLIS
import org.scalafmt.config.DanglingParentheses.Exclude.`extension`

type WindowTime = Long // EpochMillis but adjusted to start of window
type MillisecondDuration = Long

type EdgeHash = Long // Edge hashes - correctly balanced edges will reconcile to zero

object ReconciliationState {
  def apply(config: EdgeReconciliationConfig): ReconciliationState =
    ReconciliationState(
      windowSize = config.windowSize.get(MILLIS),
      windowExpiry = config.windowExpiry.get(MILLIS),
      first = StartOfTime,
      windows = Array.empty[EdgeHash]
    )

  def toWindowTime(eventTime: EventTime, windowSize: MillisecondDuration): WindowTime =
    eventTime - (eventTime % windowSize)
}

final case class ReconciliationState(
    windowSize: MillisecondDuration,
    windowExpiry: MillisecondDuration,
    first: WindowTime,
    windows: Array[EdgeHash]
) {

  extension (eventTime: EventTime)
    def toWindowTime: WindowTime = ReconciliationState.toWindowTime(eventTime, windowSize)

  def addChunk(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): URIO[Clock & EdgeReconciliationDataService, ReconciliationState] =
    for {
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      expiryThreshold = ReconciliationState.toWindowTime(now - windowExpiry, windowSize)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireWindows(expiryThreshold)
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    } yield stateWithNewEvents

  def windowStarts: Seq[WindowTime] = (0 to windows.length).map(_ + first)

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowTime
  ): URIO[EdgeReconciliationDataService, Chunk[EdgeReconciliationEvent]] = {

    extension (eventTime: EventTime) def isExpired: Boolean = eventTime.toWindowTime < expiryThreshold

    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      eventsWithExpiredRemoved <-
        if (edgeReconciliationEvents.exists(_.atTime.isExpired)) {
          ZIO.foreach(edgeReconciliationEvents.filter(_.atTime.isExpired).toVector) { e =>
            ZIO.logWarning("Edge reconciliation processed for an expired event; window is likely to be inconsistent")
            @@ eventTimeAnnotation (e.atTime) @@ eventWindowTimeAnnotation(e.atTime.toWindowTime)
              *> edgeReconciliationDataService.runMarkWindow(
                EdgeReconciliation.inconsistent(e.atTime.toWindowTime, windowSize)
              )
          } @@ expiryThresholdAnnotation(expiryThreshold) @@ windowSizeAnnotation(windowSize)
          // Keep the events that are not expired
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.atTime.isExpired))
        } else ZIO.succeed(edgeReconciliationEvents)
    } yield eventsWithExpiredRemoved
  }

  def expireWindows(expiryThreshold: WindowTime): URIO[EdgeReconciliationDataService, ReconciliationState] =
    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      reconciliationStateWithExpiredWindowsRemoved <-
        if (first >= windowExpiry) ZIO.succeed(this)
        else {
          val expiredWindows = windows.zip(windowStarts)

          ZIO.foreach(expiredWindows.takeWhile { case (_, windowTime) => windowTime < windowExpiry }) {
            case (edgeHash, windowTime) =>
              if (edgeHash == 0L)
                ZIO.logInfo("Edge hash reconciled.")
                @@ windowTimeAnnotation (windowTime)
                  *> edgeReconciliationDataService.runMarkWindow(
                    EdgeReconciliation.reconciled(windowTime, windowSize)
                  )
              else
                ZIO.logWarning("Edge hash failed to reconcile before window expiry; window is not consistent")
                @@ windowTimeAnnotation (windowTime) @@ edgeHashAnnotation(edgeHash)
                  *> edgeReconciliationDataService.runMarkWindow(
                    EdgeReconciliation.inconsistent(windowTime, windowSize)
                  )
          } *> ZIO.succeed(
            copy(
              first = expiredWindows.dropWhile(_._2 < windowExpiry).headOption.fold(StartOfTime)(_._2),
              windows = expiredWindows.dropWhile(_._2 >= windowExpiry).map(_._1)
            )
          )
        } @@ windowSizeAnnotation(windowSize) @@ expiryThresholdAnnotation(expiryThreshold)

    } yield reconciliationStateWithExpiredWindowsRemoved

  def mergeInNewEvents(
      state: ReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): ReconciliationState = {
    val minEventTime = events.minBy(_.atTime).atTime.toWindowTime
    val maxEventTime = events.minBy(_.atTime).atTime.toWindowTime

    val firstSlot = state.first min minEventTime
    val lastSlot = (state.first + windowSize * state.windows.length) max maxEventTime

    val newWindows = Array.ofDim[EdgeHash](((lastSlot - firstSlot) / windowSize).toInt)
    Array.copy(
      src = state.windows,
      srcPos = 0,
      dest = newWindows,
      destPos = ((state.first - minEventTime) / state.windowSize).toInt,
      length = state.windows.length
    )

    events.foreach { edgeReconciliationEvent =>
      val index: Int = (edgeReconciliationEvent.atTime.toWindowTime / windowSize).toInt
      newWindows(index) ^= edgeReconciliationEvent.edgeHash
    }

    state.copy(first = firstSlot, windows = newWindows)
  }

  private val expiryThresholdAnnotation = LogAnnotation[WindowTime]("expiryThreshold", (_, x) => x, _.toString)
  private val windowSizeAnnotation = LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
  private val eventTimeAnnotation = LogAnnotation[EventTime]("eventTime", (_, x) => x, _.toString)
  private val eventWindowTimeAnnotation = LogAnnotation[WindowTime]("eventWindowTime", (_, x) => x, _.toString)
  private val windowTimeAnnotation = LogAnnotation[WindowTime]("windowTime", (_, x) => x, _.toString)
  private val edgeHashAnnotation = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)
}
