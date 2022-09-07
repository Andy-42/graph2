package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.logging.LogAnnotation
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

type WindowStart = Long // Epoch millis adjusted to start of window
type WindowEnd = Long // Epoch millis adjusted to last period in window
type MillisecondDuration = Long

object EdgeReconciliationState:

  def apply(config: EdgeReconciliationConfig): EdgeReconciliationState =
    EdgeReconciliationState(
      windowSize = config.windowSize.get(MILLIS),
      windowExpiry = config.windowExpiry.get(MILLIS),
      firstWindowStart = StartOfTime,
      edgeHashes = Array.empty[EdgeHash]
    )

  def toWindowStart(eventTime: EventTime, windowSize: MillisecondDuration): WindowStart =
    eventTime - (eventTime % windowSize)

  def toWindowEnd(eventTime: EventTime, windowSize: MillisecondDuration): WindowEnd =
    toWindowStart(eventTime, windowSize) + windowSize - 1

final case class EdgeReconciliationState(
    windowSize: MillisecondDuration,
    windowExpiry: MillisecondDuration,
    firstWindowStart: WindowStart,
    edgeHashes: Array[EdgeHash]
):

  extension (eventTime: EventTime)
    def toWindowStart: WindowStart = EdgeReconciliationState.toWindowStart(eventTime, windowSize)
    def toWindowEnd: WindowEnd = EdgeReconciliationState.toWindowEnd(eventTime, windowSize)

  def addChunk(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): URIO[Clock & EdgeReconciliationDataService, EdgeReconciliationState] =
    for
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      expiryThreshold = toWindowEnd(now - windowExpiry)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireReconciliationWindows(expiryThreshold)
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    yield stateWithNewEvents

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowStart
  ): URIO[EdgeReconciliationDataService, Chunk[EdgeReconciliationEvent]] =

    // An event is expired if the last period in the window is expired
    extension (eventTime: EventTime) def isExpired: Boolean = eventTime.toWindowEnd < expiryThreshold

    for
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      eventsWithExpiredRemoved <-
        if edgeReconciliationEvents.exists(_.atTime.isExpired) then
          ZIO.foreach(edgeReconciliationEvents.filter(_.atTime.isExpired).toVector) { e =>
            ZIO.logWarning("Edge reconciliation processed for an expired event; window is likely to be inconsistent")
            @@ eventTimeAnnotation (e.atTime) @@ eventWindowTimeAnnotation(e.atTime.toWindowStart)
              *> edgeReconciliationDataService.runMarkWindow(
                EdgeReconciliation.inconsistent(e.atTime.toWindowStart, windowSize)
              )
          } @@ expiryThresholdAnnotation(expiryThreshold) @@ windowSizeAnnotation(windowSize)
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.atTime.isExpired)) // Keep the events that are not expired
        else ZIO.succeed(edgeReconciliationEvents)
    yield eventsWithExpiredRemoved

  def expireReconciliationWindows(
      expiryThreshold: WindowStart
  ): URIO[EdgeReconciliationDataService, EdgeReconciliationState] =

    extension (i: Int)
      def indexToWindowStart: WindowStart = i * windowSize + firstWindowStart
      def indexToWindowEnd: WindowStart = i.indexToWindowStart + windowSize - 1
      def isExpired: Boolean = i.indexToWindowEnd < expiryThreshold

    val expiredWindowCount = edgeHashes.indices.count(_.isExpired)

    if expiredWindowCount == 0 then ZIO.succeed(this)
    else
      for
        edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
        reconciliationStateWithExpiredWindowsRemoved <-
          ZIO.foreach(edgeHashes.take(expiredWindowCount).zipWithIndex) { case (edgeHash, i) =>
            if edgeHash == 0L then
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
                  if expiredWindowCount == edgeHashes.length then StartOfTime
                  else firstWindowStart + windowSize * expiredWindowCount,
                edgeHashes = edgeHashes.drop(expiredWindowCount)
              )
            )
      yield reconciliationStateWithExpiredWindowsRemoved

  def mergeInNewEvents(
      state: EdgeReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): EdgeReconciliationState =
    val minEventWindowStart = events.minBy(_.atTime).atTime.toWindowStart
    val maxEventWindowStart = events.minBy(_.atTime).atTime.toWindowStart

    def slotsBetweenWindows(first: WindowStart, second: WindowStart): Int =
      require(second >= first)
      ((second - first) / state.windowSize).toInt

    if state.edgeHashes.isEmpty then
      assert(state.firstWindowStart == StartOfTime)

      state.copy(
        firstWindowStart = minEventWindowStart,
        edgeHashes =
          Array.ofDim[EdgeHash](slotsBetweenWindows(first = minEventWindowStart, second = maxEventWindowStart))
      )
    else
      // Create a new edgeHashes that can accomodate the existing state and all the incoming events
      val firstWindowStart = state.firstWindowStart min minEventWindowStart
      val lastWindowStart = (state.firstWindowStart + windowSize * state.edgeHashes.length) max maxEventWindowStart
      val edgeHashes = Array.ofDim[EdgeHash](slotsBetweenWindows(firstWindowStart, lastWindowStart))
      Array.copy(
        src = state.edgeHashes,
        srcPos = 0,
        dest = edgeHashes,
        destPos = slotsBetweenWindows(first = firstWindowStart, second = state.firstWindowStart),
        length = state.edgeHashes.length
      )

      // Merge in the edge hashes from incoming events
      events.foreach { edgeReconciliationEvent =>
        val i = slotsBetweenWindows(first = firstWindowStart, second = edgeReconciliationEvent.atTime.toWindowStart)
        edgeHashes(i) ^= edgeReconciliationEvent.edgeHash
      }

      state.copy(
        firstWindowStart = firstWindowStart,
        edgeHashes = edgeHashes
      )

  private val expiryThresholdAnnotation = LogAnnotation[WindowStart]("expiryThreshold", (_, x) => x, _.toString)
  private val windowSizeAnnotation = LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
  private val eventTimeAnnotation = LogAnnotation[EventTime]("eventTime", (_, x) => x, _.toString)
  private val eventWindowTimeAnnotation = LogAnnotation[WindowStart]("eventWindowTime", (_, x) => x, _.toString)
  private val windowStartAnnotation = LogAnnotation[WindowStart]("windowStart", (_, x) => x, _.toString)
  private val edgeHashAnnotation = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)
