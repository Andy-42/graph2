package andy42.graph.cache

import andy42.graph.model._
import zio._
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
      first = StartOfTime,
      windows = Array.empty[EdgeHash]
    )
}

final case class ReconciliationState(
    windowSize: MillisecondDuration,
    windowExpiry: MillisecondDuration,
    first: WindowTime,
    windows: Array[EdgeHash]
) {
  def addChunk(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent]
  ): URIO[Clock & EdgeReconciliationDataService, ReconciliationState] =
    for {
      clock <- ZIO.service[Clock]
      // edgeReconciliation <- ZIO.service[EdgeReconciliationDataService]
      now <- clock.currentTime(MILLIS)
      expiryThreshold = toWindowTime(now - windowExpiry)
      currentEvents <- handleAndRemoveExpiredEvents(edgeReconciliationEvents, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireWindows
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    } yield stateWithNewEvents

  def toWindowTime(eventTime: EventTime): WindowTime = eventTime - (eventTime % windowSize)

  def windowStarts: Seq[WindowTime] = (0 to windows.length).map(_ + first)

  def handleAndRemoveExpiredEvents(
      edgeReconciliationEvents: Chunk[EdgeReconciliationEvent],
      expiryThreshold: WindowTime
  ): URIO[EdgeReconciliationDataService, Chunk[EdgeReconciliationEvent]] =
    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      eventsWithExpiredRemoved <-
        if (edgeReconciliationEvents.exists(e => toWindowTime(e.atTime) < expiryThreshold))
          ZIO.foreach(edgeReconciliationEvents.filter(e => toWindowTime(e.atTime) < expiryThreshold).toVector)(e =>
            ZIO.logWarning(s"TODO: Some JSON message about this expired event")
              *> edgeReconciliationDataService.runMarkWindow(
                EdgeReconciliation.broken(toWindowTime(e.atTime), windowSize)
              )
          ) *> ZIO.succeed(edgeReconciliationEvents.filter(e => toWindowTime(e.atTime) >= expiryThreshold))
        else ZIO.succeed(edgeReconciliationEvents)

    } yield eventsWithExpiredRemoved

  def expireWindows: URIO[EdgeReconciliationDataService, ReconciliationState] =
    for {
      edgeReconciliationDataService <- ZIO.service[EdgeReconciliationDataService]
      reconciliationStateWithExpiredWindowsRemoved <-
        if (first >= windowExpiry) ZIO.succeed(this)
        else {
          val expiredWindows = windows.zip(windowStarts)

          ZIO.foreach(expiredWindows.takeWhile { case (_, windowTime) => windowTime < windowExpiry }) {
            case (edgeHash, windowTime) =>
              if (edgeHash == 0L)
                ZIO.logInfo(s"TODO: EdgeHash for window $windowTime reconciled correctly.")
                  *> edgeReconciliationDataService.runMarkWindow(EdgeReconciliation.reconciled(windowTime, windowSize))
              else
                ZIO.logWarning(s"TODO: EdgeHash for window $windowTime did not reconcile in time.")
                  *> edgeReconciliationDataService.runMarkWindow(EdgeReconciliation.broken(windowTime, windowSize))
          } *> ZIO.succeed(
            copy(
              first = expiredWindows.dropWhile(_._2 < windowExpiry).headOption.fold(StartOfTime)(_._2),
              windows = expiredWindows.dropWhile(_._2 >= windowExpiry).map(_._1)
            )
          )
        }
    } yield reconciliationStateWithExpiredWindowsRemoved

  def mergeInNewEvents(
      state: ReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): ReconciliationState = {
    val minEventTime = toWindowTime(events.minBy(_.atTime).atTime)
    val maxEventTime = toWindowTime(events.minBy(_.atTime).atTime)

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
      val index: Int = (toWindowTime(edgeReconciliationEvent.atTime) / windowSize).toInt
      newWindows(index) ^= edgeReconciliationEvent.edgeHash
    }

    state.copy(first = firstSlot, windows = newWindows)
  }
}
