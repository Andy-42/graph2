package andy42.graph.services

import andy42.graph.config.{AppConfig, EdgeReconciliationConfig}
import andy42.graph.model.*
import andy42.graph.persistence.{EdgeReconciliationRepository, EdgeReconciliationSnapshot, PersistenceFailure}
import zio.*
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

/** Epoch millis adjusted to start of window */
type WindowStart = Long

/** Epoch millis adjusted to last period in window */
type WindowEnd = Long
type MillisecondDuration = Long

trait EdgeReconciliation:

  def zero: EdgeReconciliationWindowState

  def addChunk(
      state: EdgeReconciliationWindowState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): IO[PersistenceFailure, EdgeReconciliationWindowState]

case class EdgeReconciliationWindowState(
    firstWindowStart: WindowStart,
    edgeHashes: Array[EdgeHash]
)

// TODO: Need to have retry and logging for persistence failures

case class EdgeReconciliationLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationDataService: EdgeReconciliationRepository
) extends EdgeReconciliation:

  val windowSize: Long = config.windowSize.toMillis
  val windowExpiry: Long = config.windowExpiry.toMillis

  extension (time: EventTime)
    def toWindowStart: EventTime = time - (time % windowSize)
    def toWindowEnd: EventTime = time.toWindowStart + windowSize - 1

  override def zero: EdgeReconciliationWindowState =
    EdgeReconciliationWindowState(
      firstWindowStart = StartOfTime,
      edgeHashes = Array.empty[EdgeHash]
    )

  override def addChunk(
      state: EdgeReconciliationWindowState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): IO[PersistenceFailure, EdgeReconciliationWindowState] =
    for
      now <- Clock.currentTime(MILLIS)
      expiryThreshold = toWindowEnd(now - windowExpiry)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold, now)
      stateWithExpiredWindowsRemoved <- expireReconciliationWindows(state, expiryThreshold, now)
      nextState =
        if currentEvents.isEmpty then stateWithExpiredWindowsRemoved
        else mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    yield nextState

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowStart,
      whenWritten: Long
  ): IO[PersistenceFailure, Chunk[EdgeReconciliationEvent]] =
    import LogAnnotations.*

    // An event is expired if the last period in the window is expired
    extension (time: EventTime) def isExpired: Boolean = time.toWindowEnd < expiryThreshold

    for eventsWithExpiredRemoved <-
        if edgeReconciliationEvents.exists(_.time.isExpired) then
          ZIO.foreach(edgeReconciliationEvents.filter(_.time.isExpired).toVector) { e =>
            ZIO.logWarning("Edge reconciliation processed for an expired event; window is likely to be inconsistent")
            @@ eventTimeAnnotation (e.time) @@ eventWindowTimeAnnotation(e.time.toWindowStart)
              *> edgeReconciliationDataService.markWindow(
                EdgeReconciliationSnapshot.inconsistent(
                  windowStart = e.time.toWindowStart,
                  windowSize = windowSize,
                  now = whenWritten
                )
              )
          } @@ expiryThresholdAnnotation(expiryThreshold) @@ windowSizeAnnotation(windowSize)
            *> ZIO.succeed(edgeReconciliationEvents.filter(!_.time.isExpired)) // Keep the events that are not expired
        else ZIO.succeed(edgeReconciliationEvents)
    yield eventsWithExpiredRemoved

  def expireReconciliationWindows(
      state: EdgeReconciliationWindowState,
      expiryThreshold: WindowStart,
      whenWritten: Long
  ): IO[PersistenceFailure, EdgeReconciliationWindowState] =
    import LogAnnotations.*

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
                  EdgeReconciliationSnapshot.reconciled(i.indexToWindowStart, windowSize, whenWritten)
                )
            else
              ZIO.logWarning("Edge hash failed to reconcile before window expiry; window is not consistent")
              @@ windowStartAnnotation (i.indexToWindowStart) @@ edgeHashAnnotation(edgeHash)
                *> edgeReconciliationDataService.markWindow(
                  EdgeReconciliationSnapshot.inconsistent(i.indexToWindowStart, windowSize, whenWritten)
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
      state: EdgeReconciliationWindowState,
      events: Chunk[EdgeReconciliationEvent]
  ): EdgeReconciliationWindowState =
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

end EdgeReconciliationLive

object EdgeReconciliation:
  val layer: RLayer[AppConfig & EdgeReconciliationRepository, EdgeReconciliation] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        dataService <- ZIO.service[EdgeReconciliationRepository]
      yield EdgeReconciliationLive(config.edgeReconciliation, dataService)
    }
