package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

type WindowStart = Long // Epoch millis adjusted to start of window
type WindowEnd = Long // Epoch millis adjusted to last period in window
type MillisecondDuration = Long

trait EdgeReconciliationProcessor:

  def zero: EdgeReconciliationState
  
  def addChunk(
      state: EdgeReconciliationState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): UIO[EdgeReconciliationState]

case class EdgeReconciliationState(
    firstWindowStart: WindowStart,
    edgeHashes: Array[EdgeHash]
)

case class EdgeReconciliationProcessorLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationDataService: EdgeReconciliationRepository
) extends EdgeReconciliationProcessor:

  val windowSize: Long = config.windowSize.get(MILLIS)
  val windowExpiry: Long = config.windowExpiry.get(MILLIS)

  extension (time: EventTime)
    def toWindowStart = time - (time % windowSize)
    def toWindowEnd = time.toWindowStart + windowSize - 1

  override def zero: EdgeReconciliationState =
    EdgeReconciliationState(
      firstWindowStart = StartOfTime,
      edgeHashes = Array.empty[EdgeHash]
    )

  override def addChunk( 
      state: EdgeReconciliationState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): UIO[EdgeReconciliationState] =
    for
      now <- Clock.currentTime(MILLIS)
      expiryThreshold = toWindowEnd(now - windowExpiry)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireReconciliationWindows(state, expiryThreshold)
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    yield stateWithNewEvents

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowStart
  ): UIO[Chunk[EdgeReconciliationEvent]] =
    import LogAnnotations._

    // An event is expired if the last period in the window is expired
    extension (time: EventTime) def isExpired: Boolean = time.toWindowEnd < expiryThreshold

    for eventsWithExpiredRemoved <-
        if edgeReconciliationEvents.exists(_.time.isExpired) then
          ZIO.foreach(edgeReconciliationEvents.filter(_.time.isExpired).toVector) { e =>
            ZIO.logWarning("Edge reconciliation processed for an expired event; window is likely to be inconsistent")
            @@ eventTimeAnnotation (e.time) @@ eventWindowTimeAnnotation(e.time.toWindowStart)
              *> edgeReconciliationDataService.markWindow(
                EdgeReconciliationSnapshot.inconsistent(e.time.toWindowStart, windowSize)
              )
          } @@ expiryThresholdAnnotation(expiryThreshold) @@ windowSizeAnnotation(windowSize)
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.time.isExpired)) // Keep the events that are not expired
        else ZIO.succeed(edgeReconciliationEvents)
    yield eventsWithExpiredRemoved

  def expireReconciliationWindows(
      state: EdgeReconciliationState,
      expiryThreshold: WindowStart
  ): UIO[EdgeReconciliationState] =
    import LogAnnotations._

    extension (i: Int)
      def indexToWindowStart: WindowStart = i * windowSize + state.firstWindowStart
      def indexToWindowEnd: WindowStart = i.indexToWindowStart + windowSize - 1
      def isExpired: Boolean = i.indexToWindowEnd < expiryThreshold

    val expiredWindowCount = state.edgeHashes.indices.count(_.isExpired)

    if expiredWindowCount == 0 then ZIO.succeed(state)
    else
      for reconciliationStateWithExpiredWindowsRemoved <-
          ZIO.foreach(state.edgeHashes.take(expiredWindowCount).zipWithIndex) { (edgeHash, i) =>
            if edgeHash == 0L then
              ZIO.logInfo("Edge hash reconciled.")
              @@ windowStartAnnotation (i.indexToWindowStart)
                *> edgeReconciliationDataService.markWindow(
                  EdgeReconciliationSnapshot.reconciled(i.indexToWindowStart, windowSize)
                )
            else
              ZIO.logWarning("Edge hash failed to reconcile before window expiry; window is not consistent")
              @@ windowStartAnnotation (i.indexToWindowStart) @@ edgeHashAnnotation(edgeHash)
                *> edgeReconciliationDataService.markWindow(
                  EdgeReconciliationSnapshot.inconsistent(i.indexToWindowStart, windowSize)
                )
          } @@ windowSizeAnnotation(windowSize) @@ expiryThresholdAnnotation(expiryThreshold)
            *> ZIO.succeed(
              state.copy(
                firstWindowStart =
                  if expiredWindowCount == state.edgeHashes.length then StartOfTime
                  else state.firstWindowStart + windowSize * expiredWindowCount,
                edgeHashes = state.edgeHashes.drop(expiredWindowCount)
              )
            )
      yield reconciliationStateWithExpiredWindowsRemoved

  def mergeInNewEvents(
      state: EdgeReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): EdgeReconciliationState =
    val minEventWindowStart = events.minBy(_.time).time.toWindowStart
    val maxEventWindowStart = events.minBy(_.time).time.toWindowStart

    def slotsBetweenWindows(first: WindowStart, second: WindowStart): Int =
      require(second >= first)
      ((second - first) / windowSize).toInt

    if state.edgeHashes.isEmpty then
      assert(state.firstWindowStart == StartOfTime)

      state.copy(
        firstWindowStart = minEventWindowStart,
        edgeHashes =
          Array.ofDim[EdgeHash](slotsBetweenWindows(first = minEventWindowStart, second = maxEventWindowStart))
      )
    else
      // Create a new edgeHashes that can accommodate the existing state and all the incoming events
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
        val i = slotsBetweenWindows(first = firstWindowStart, second = edgeReconciliationEvent.time.toWindowStart)
        edgeHashes(i) ^= edgeReconciliationEvent.edgeHash
      }

      state.copy(
        firstWindowStart = firstWindowStart,
        edgeHashes = edgeHashes
      )

end EdgeReconciliationProcessorLive

object EdgeReconciliation:
  val layer: RLayer[EdgeReconciliationConfig & EdgeReconciliationRepository, EdgeReconciliationProcessor] =
    ZLayer {
      for
        config <- ZIO.service[EdgeReconciliationConfig]
        dataService <- ZIO.service[EdgeReconciliationRepository]
      yield EdgeReconciliationProcessorLive(config, dataService)
    }
