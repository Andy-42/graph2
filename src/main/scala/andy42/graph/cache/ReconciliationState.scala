package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.logging.LogAnnotation
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

type WindowTime = Long // EpochMillis but adjusted to start of window
type MillisecondDuration = Long

type EdgeHash = Long // Edge hashes - correctly balanced edges will reconcile to zero

object ReconciliationState {
  def apply(config: EdgeReconciliationConfig): ReconciliationState =
    ReconciliationState(
      windowSize = config.windowSize.get(MILLIS),
      windowExpiry = config.windowExpiry.get(MILLIS),
      firstWindowStart = StartOfTime,
      edgeHashes = Array.empty[EdgeHash]
    )

  def toWindowTime(eventTime: EventTime, windowSize: MillisecondDuration): WindowTime =
    eventTime - (eventTime % windowSize)
}

final case class ReconciliationState(
    windowSize: MillisecondDuration,
    windowExpiry: MillisecondDuration,
    firstWindowStart: WindowTime,
    edgeHashes: Array[EdgeHash]
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
      stateWithExpiredWindowsRemoved <- expireReconciliationWindows(expiryThreshold)
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    } yield stateWithNewEvents

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
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.atTime.isExpired)) // Keep the events that are not expired
        } else ZIO.succeed(edgeReconciliationEvents)
    } yield eventsWithExpiredRemoved
  }

  def expireReconciliationWindows(
      expiryThreshold: WindowTime
  ): URIO[EdgeReconciliationDataService, ReconciliationState] =
    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      reconciliationStateWithExpiredWindowsRemoved <-
        if (firstWindowStart >= windowExpiry) ZIO.succeed(this)
        else {

          def windowStarts: Seq[WindowTime] = (0 until edgeHashes.length).map(_ * windowSize + firstWindowStart)
          val edgeHashAndWindowStart = edgeHashes.zip(windowStarts)

          ZIO.foreach(edgeHashAndWindowStart.takeWhile { case (_, windowStart) => windowStart < windowExpiry }) {
            case (edgeHash, windowStart) =>
              if (edgeHash == 0L)
                ZIO.logInfo("Edge hash reconciled.")
                @@ windowStartAnnotation (windowStart)
                  *> edgeReconciliationDataService.runMarkWindow(
                    EdgeReconciliation.reconciled(windowStart, windowSize)
                  )
              else
                ZIO.logWarning("Edge hash failed to reconcile before window expiry; window is not consistent")
                @@ windowStartAnnotation (windowStart) @@ edgeHashAnnotation(edgeHash)
                  *> edgeReconciliationDataService.runMarkWindow(
                    EdgeReconciliation.inconsistent(windowStart, windowSize)
                  )
          } @@ windowSizeAnnotation(windowSize) @@ expiryThresholdAnnotation(expiryThreshold)
            *> ZIO.succeed(
              copy(
                firstWindowStart = edgeHashAndWindowStart
                  .dropWhile { case (_, windowStart) => windowStart < windowExpiry }
                  .headOption
                  .fold(StartOfTime) { case (_, windowStart) => windowStart },
                edgeHashes = edgeHashAndWindowStart
                  .dropWhile { case (_, windowStart) => windowStart < windowExpiry }
                  .map { case (edgeHash, _) => edgeHash }
              )
            )
        }

    } yield reconciliationStateWithExpiredWindowsRemoved

  def mergeInNewEvents(
      state: ReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): ReconciliationState = {
    val minEventTime = events.minBy(_.atTime).atTime.toWindowTime
    val maxEventTime = events.minBy(_.atTime).atTime.toWindowTime

    val firstSlot = state.firstWindowStart min minEventTime
    val lastSlot = (state.firstWindowStart + windowSize * state.edgeHashes.length) max maxEventTime

    val newWindows = Array.ofDim[EdgeHash](((lastSlot - firstSlot) / windowSize).toInt)
    Array.copy(
      src = state.edgeHashes,
      srcPos = 0,
      dest = newWindows,
      destPos = ((state.firstWindowStart - minEventTime) / state.windowSize).toInt,
      length = state.edgeHashes.length
    )

    events.foreach { edgeReconciliationEvent =>
      val index: Int = (edgeReconciliationEvent.atTime.toWindowTime / windowSize).toInt
      newWindows(index) ^= edgeReconciliationEvent.edgeHash
    }

    state.copy(firstWindowStart = firstSlot, edgeHashes = newWindows)
  }

  private val expiryThresholdAnnotation = LogAnnotation[WindowTime]("expiryThreshold", (_, x) => x, _.toString)
  private val windowSizeAnnotation = LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
  private val eventTimeAnnotation = LogAnnotation[EventTime]("eventTime", (_, x) => x, _.toString)
  private val eventWindowTimeAnnotation = LogAnnotation[WindowTime]("eventWindowTime", (_, x) => x, _.toString)
  private val windowStartAnnotation = LogAnnotation[WindowTime]("windowStart", (_, x) => x, _.toString)
  private val edgeHashAnnotation = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)
}
