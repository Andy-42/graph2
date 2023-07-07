package andy42.graph.services

import andy42.graph.config.{AppConfig, EdgeReconciliationConfig}
import andy42.graph.model.*
import andy42.graph.persistence.{EdgeReconciliationRepository, EdgeReconciliationSnapshot, PersistenceFailure}
import zio.*
import zio.stream.ZStream
import scala.collection.mutable

import java.time.temporal.ChronoUnit.MILLIS

/** Epoch millis adjusted to start of window */
type WindowStart = Long

type MillisecondDuration = Long

trait EdgeReconciliation:

  def zero: EdgeReconciliationWindowState

  def addChunk(
      state: EdgeReconciliationWindowState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): IO[PersistenceFailure, EdgeReconciliationWindowState]

case class WindowState(
    time: WindowStart,
    lastActivity: Long, // clock time
    hash: EdgeHash
)

type EdgeReconciliationWindowState = Map[WindowStart, WindowState]

// TODO: Need to have retry and logging for persistence failures

case class EdgeReconciliationLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationDataService: EdgeReconciliationRepository
) extends EdgeReconciliation:

  val windowSize: MillisecondDuration = config.windowSize.toMillis
  val windowExpiry: MillisecondDuration = config.windowExpiry.toMillis

  extension (time: EventTime) def toWindowStart: EventTime = time - (time % windowSize)

  override val zero: EdgeReconciliationWindowState = Map.empty

  override def addChunk(
      state: EdgeReconciliationWindowState,
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): IO[PersistenceFailure, EdgeReconciliationWindowState] =
    for
      timeOfReconciliation <- Clock.currentTime(MILLIS)
      stateWithNewEvents = mergeEventsIntoState(timeOfReconciliation, state, edgeReconciliationEvents)
      nextState <- handleAndRemoveExpiredEvents(timeOfReconciliation, stateWithNewEvents)
    yield nextState

  private def mergeEventsIntoState(
      timeOfReconciliation: Long,
      state: EdgeReconciliationWindowState,
      events: Chunk[EdgeReconciliationEvent]
  ): EdgeReconciliationWindowState =
    val tempMap = mutable.Map.empty[WindowStart, Array[EdgeHash]]

    events.foreach { event =>
      val windowStart = event.time.toWindowStart
      tempMap.get(windowStart) match
        case None               => tempMap += windowStart -> Array(event.edgeHash)
        case Some(existingHash) => existingHash(0) ^= event.edgeHash
    }

    state ++ tempMap.iterator.map { case (startTime, hash) =>
      val nextState = state.get(startTime) match
        case None => WindowState(time = startTime, lastActivity = timeOfReconciliation, hash = hash(0))
        case Some(existingWindowState) =>
          existingWindowState.copy(lastActivity = timeOfReconciliation, hash = existingWindowState.hash ^ hash(0))

      startTime -> nextState
    }

  private def handleAndRemoveExpiredEvents(
      timeOfReconciliation: Long,
      state: EdgeReconciliationWindowState
  ): IO[PersistenceFailure, EdgeReconciliationWindowState] =
    import LogAnnotations.*

    val expiryThreshold = timeOfReconciliation - windowExpiry
    val (expired, current) = state.partition((_, windowState) => windowState.lastActivity < expiryThreshold)

    for _ <- ZIO.foreachParDiscard(expired.values) { windowState =>
        if windowState.hash == 0L then
          ZIO.logInfo("Edge hash reconciled.")
          @@ windowStartAnnotation (windowState.time)
            *> edgeReconciliationDataService.markWindow(
              EdgeReconciliationSnapshot
                .reconciled(
                  windowStart = windowState.time,
                  windowSize = windowSize,
                  now = timeOfReconciliation
                )
            )
        else
          ZIO.logWarning("Edge hash was non-zero at window expiry; edges may not be consistent")
          @@ windowStartAnnotation (windowState.time) @@ edgeHashAnnotation(windowState.hash)
            *> edgeReconciliationDataService.markWindow(
              EdgeReconciliationSnapshot.inconsistent(
                windowStart = windowState.time,
                windowSize = windowSize,
                now = timeOfReconciliation
              )
            )
      } @@ windowSizeAnnotation(windowSize) @@ expiryThresholdAnnotation(expiryThreshold)
    yield current

end EdgeReconciliationLive

object EdgeReconciliation:
  val layer: RLayer[AppConfig & EdgeReconciliationRepository, EdgeReconciliation] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        dataService <- ZIO.service[EdgeReconciliationRepository]
      yield EdgeReconciliationLive(config.edgeReconciliation, dataService)
    }
