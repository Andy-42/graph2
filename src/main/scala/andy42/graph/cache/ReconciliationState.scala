package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.logging.LogAnnotation
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

type WindowStart = Long // EpochMillis but adjusted to start of window
type WindowEnd = Long
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

  def toWindowStart(eventTime: EventTime, windowSize: MillisecondDuration): WindowStart =
    eventTime - (eventTime % windowSize)

  def toWindowEnd(eventTime: EventTime, windowSize: MillisecondDuration): WindowEnd =
    toWindowStart(eventTime, windowSize) + windowSize - 1
}

final case class ReconciliationState(
    windowSize: MillisecondDuration,
    windowExpiry: MillisecondDuration,
    firstWindowStart: WindowStart,
    edgeHashes: Array[EdgeHash]
) {

  extension (eventTime: EventTime)
    def toWindowStart: WindowStart = ReconciliationState.toWindowStart(eventTime, windowSize)
    def toWindowEnd: WindowEnd = ReconciliationState.toWindowEnd(eventTime, windowSize)

  def addChunk(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): URIO[Clock & EdgeReconciliationDataService, ReconciliationState] =
    for {
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      expiryThreshold = ReconciliationState.toWindowStart(now - windowExpiry, windowSize)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireReconciliationWindows(expiryThreshold)
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    } yield stateWithNewEvents

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowStart
  ): URIO[EdgeReconciliationDataService, Chunk[EdgeReconciliationEvent]] = {

    extension (eventTime: EventTime) def isExpired: Boolean = eventTime.toWindowEnd < expiryThreshold

    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      eventsWithExpiredRemoved <-
        if (edgeReconciliationEvents.exists(_.atTime.isExpired)) {
          ZIO.foreach(edgeReconciliationEvents.filter(_.atTime.isExpired).toVector) { e =>
            ZIO.logWarning("Edge reconciliation processed for an expired event; window is likely to be inconsistent")
            @@ eventTimeAnnotation (e.atTime) @@ eventWindowTimeAnnotation(e.atTime.toWindowStart)
              *> edgeReconciliationDataService.runMarkWindow(
                EdgeReconciliation.inconsistent(e.atTime.toWindowStart, windowSize)
              )
          } @@ expiryThresholdAnnotation(expiryThreshold) @@ windowSizeAnnotation(windowSize)
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.atTime.isExpired)) // Keep the events that are not expired
        } else ZIO.succeed(edgeReconciliationEvents)
    } yield eventsWithExpiredRemoved
  }

  def expireReconciliationWindows(
      expiryThreshold: WindowStart
  ): URIO[EdgeReconciliationDataService, ReconciliationState] = {

    extension (i: Int)
      def indexToWindowStart: WindowStart = i * windowSize + firstWindowStart
      def indexToWindowEnd: WindowStart = i.indexToWindowStart + windowSize - 1
      def isExpired: Boolean = i.indexToWindowEnd < expiryThreshold

    val expiredWindowCount = edgeHashes.indices.count(_.isExpired)

    if (expiredWindowCount == 0) ZIO.succeed(this)
    else
      for {
        edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
        reconciliationStateWithExpiredWindowsRemoved <-
          ZIO.foreach(edgeHashes.take(expiredWindowCount).zipWithIndex) { case (edgeHash, i) =>
            if (edgeHash == 0L)
              ZIO.logInfo("Edge hash reconciled.")
              @@ windowStartAnnotation (i.indexToWindowStart)
                *> edgeReconciliationDataService.runMarkWindow(
                  EdgeReconciliation.reconciled(i.indexToWindowStart, windowSize)
                )
            else
              ZIO.logWarning("Edge hash failed to reconcile before window expiry; window is not consistent")
              @@ windowStartAnnotation (i.indexToWindowStart) @@ edgeHashAnnotation(edgeHash)
                *> edgeReconciliationDataService.runMarkWindow(
                  EdgeReconciliation.inconsistent(i.indexToWindowStart, windowSize)
                )
          } @@ windowSizeAnnotation(windowSize) @@ expiryThresholdAnnotation(expiryThreshold)
            *> ZIO.succeed(
              copy(
                firstWindowStart =
                  if (expiredWindowCount == edgeHashes.length) StartOfTime
                  else firstWindowStart + windowSize * expiredWindowCount,
                edgeHashes = edgeHashes.drop(expiredWindowCount)
              )
            )
      } yield reconciliationStateWithExpiredWindowsRemoved
  }

  def mergeInNewEvents(
      state: ReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): ReconciliationState = {
    val minEventTime = events.minBy(_.atTime).atTime.toWindowStart
    val maxEventTime = events.minBy(_.atTime).atTime.toWindowStart

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
      val index: Int = (edgeReconciliationEvent.atTime.toWindowStart / windowSize).toInt
      newWindows(index) ^= edgeReconciliationEvent.edgeHash
    }

    state.copy(firstWindowStart = firstSlot, edgeHashes = newWindows)
  }

  private val expiryThresholdAnnotation = LogAnnotation[WindowStart]("expiryThreshold", (_, x) => x, _.toString)
  private val windowSizeAnnotation = LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
  private val eventTimeAnnotation = LogAnnotation[EventTime]("eventTime", (_, x) => x, _.toString)
  private val eventWindowTimeAnnotation = LogAnnotation[WindowStart]("eventWindowTime", (_, x) => x, _.toString)
  private val windowStartAnnotation = LogAnnotation[WindowStart]("windowStart", (_, x) => x, _.toString)
  private val edgeHashAnnotation = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)
}