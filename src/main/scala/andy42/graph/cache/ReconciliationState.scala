package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

type WindowTime = Long // EpochMillis but adjusted to start of window
type EdgeHash = Long // Edge hashes - correctly balanced edges will reconcile to zero

object ReconciliationState {
  def apply(windowSize: Long, windowExpiry: Long): ReconciliationState =
    ReconciliationState(
      windowSize = windowSize,
      windowExpiry = windowExpiry,
      first = StartOfTime,
      windows = Array.empty[Long]
    )
}

final case class ReconciliationState(
    windowSize: Long,
    windowExpiry: Long,
    first: WindowTime,
    windows: Array[EdgeHash]
) {
  def addChunk(
      chunk: Chunk[EdgeReconciliationEvent]
  ): URIO[Clock, ReconciliationState] =
    for {
      clock <- ZIO.service[Clock]
      now <- clock.currentTime(MILLIS)
      expiryThreshold = toWindowTime(now - windowExpiry)
      currentEvents <- warnExpiredEvents(chunk, expiryThreshold)
      stateWithExpiredWindowsRemoved <- expireWindows
      stateWithNewEvents = mergeInNewEvents(stateWithExpiredWindowsRemoved, currentEvents)
    } yield stateWithNewEvents

  def toWindowTime(eventTime: EventTime): WindowTime = eventTime - (eventTime % windowSize)

  def windowStarts: Seq[WindowTime] = (0 to windows.length).map(_ + first)

  def warnExpiredEvents(
      chunk: Chunk[EdgeReconciliationEvent],
      expiryThreshold: Long
  ): UIO[Chunk[EdgeReconciliationEvent]] =
    if (chunk.exists(edgeReconciliationEvent => toWindowTime(edgeReconciliationEvent.atTime) < expiryThreshold)) {
      ZIO.foreach(chunk.filter(x => toWindowTime(x.atTime) < expiryThreshold).toVector)(edgeReconciliationEvent =>
        ZIO.logWarning(s"TODO: Some JSON message about this expired event")
        // TODO: Write to reconciliation persist that this window is not reconciled
      ) *> ZIO.succeed(chunk.filter(x => toWindowTime(x.atTime) >= expiryThreshold))
    } else ZIO.succeed(chunk)

  def expireWindows: UIO[ReconciliationState] =
    if (first >= windowExpiry) ZIO.succeed(this)
    else {
      val expiredWindows = windows.zip(windowStarts)

      ZIO.foreach(expiredWindows.takeWhile(_._2 < windowExpiry)) { case (edgeHash, windowTime) =>
        if (edgeHash == 0L)
          ZIO.logInfo(s"TODO: EdgeHash for window $windowTime reconciled correctly.")
          // TODO: Add to reconciliation persist
        else
          ZIO.logWarning(s"TODO: EdgeHash for window $windowTime did not reconcile in time.")
        // TODO: Add to reconciliation persist
      }
        *> ZIO.succeed(
          copy(
            first = expiredWindows.dropWhile(_._2 < windowExpiry).headOption.fold(StartOfTime)(_._2),
            windows = expiredWindows.dropWhile(_._2 >= windowExpiry).map(_._1)
          )
        )
    }

  def mergeInNewEvents(
      state: ReconciliationState,
      events: Chunk[EdgeReconciliationEvent]
  ): ReconciliationState = {
    val minEventTime = toWindowTime(events.minBy(_.atTime).atTime)
    val maxEventTime = toWindowTime(events.minBy(_.atTime).atTime)

    val firstSlot = state.first min minEventTime
    val lastSlot = (state.first + windowSize * state.windows.length) max maxEventTime

    val newWindows = Array.ofDim[Long](((lastSlot - firstSlot) / windowSize).toInt)
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
