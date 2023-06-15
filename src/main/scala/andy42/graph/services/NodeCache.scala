package andy42.graph.services

import andy42.graph.config.{AppConfig, NodeCacheConfig}
import andy42.graph.model.*
import zio.*
import zio.stm.*

import java.time.temporal.ChronoUnit.MILLIS

trait NodeCache:
  def get(id: NodeId): IO[UnpackFailure, Option[Node]]
  def put(node: Node): IO[UnpackFailure, Unit]

  def startCurrentSnapshotTrimDaemon: UIO[Unit]

type AccessTime = Long // epoch millis

case class CacheItem(
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,
    current: NodeSnapshot | Null,
    packed: PackedNodeHistory,
    lastAccess: AccessTime
)

final case class NodeCacheLive(
    config: NodeCacheConfig,
    watermark: TRef[AccessTime], // items.forall(_.lastAccess >= watermark)
    items: TMap[NodeId, CacheItem]
) extends NodeCache:

  override def get(id: NodeId): IO[UnpackFailure, Option[Node]] =
    for
      now <- Clock.currentTime(MILLIS)

      optionCacheItem <- items.get(id).commit

      _ <- optionCacheItem.fold(ZIO.unit)(item =>
        // A hot node item might be referenced many times in one millisecond, so only update if lastAccess would change
        ZIO.when(item.lastAccess < now) {
          val e = items.put(id, item.copy(lastAccess = now)).commit
          if config.forkOnUpdateAccessTime then e.fork else e // The safety/utility of forking here is questionable
        }
      )

      optionNode <- optionCacheItem.fold(ZIO.succeed(None)) { cacheItem =>
        if cacheItem.current != null then
          ZIO.succeed(
            Some(
              Node.fromPackedHistory(
                id = id,
                packed = cacheItem.packed,
                current = cacheItem.current
              )
            )
          )
        else
          for
            history <- NodeHistory.unpackNodeHistory(cacheItem.packed)
            snapshot = CollapseNodeHistory(history)
          yield Some(
            Node.fromHistory(
              id = id,
              history = history,
              current = snapshot,
              packed = cacheItem.packed
            )
          )
      }
    yield optionNode

  override def put(node: Node): IO[UnpackFailure, Unit] =
    for
      now <- Clock.currentTime(MILLIS)

      current <- node.current

      _ <- items
        .put(
          k = node.id,
          v = CacheItem(
            version = node.version,
            lastTime = node.lastTime,
            lastSequence = node.lastSequence,
            current = current,
            packed = node.packedHistory,
            lastAccess = now
          )
        )
        .commit

      _ <- trimIfOverCapacity(now) // TODO: fork - use the config!
    yield ()

  // TODO: Avoid a cascade of trims by capturing the watermark and comparing before trim

  private def trimIfOverCapacity(now: AccessTime): UIO[Unit] =
    for
      optionTrimOutcome <- ZSTM
        .ifSTM(items.size.map(_ > config.capacity))(trim(now), ZSTM.succeed(None))
        .commit

      _ <- optionTrimOutcome.fold(ZIO.unit)(trimOutcome =>
        ZIO.logInfo("Node cache trimmed") @@
          LogAnnotations.now(now) @@
          LogAnnotations.capacity(config.capacity) @@
          LogAnnotations.previousWatermark(trimOutcome.previousWatermark) @@
          LogAnnotations.nextWatermark(trimOutcome.nextWatermark) @@
          LogAnnotations.previousSize(trimOutcome.previousSize) @@
          LogAnnotations.nextSize(trimOutcome.nextSize)
      )
    yield ()

  case class TrimOutcome(
      previousWatermark: AccessTime,
      nextWatermark: AccessTime,
      previousSize: Int,
      nextSize: Int
  )

  private def trim(now: AccessTime): USTM[Some[TrimOutcome]] =
    for
      currentSize <- items.size
      currentWatermark <- watermark.get
      nextWatermark = moveWatermarkForward(
        now = now,
        watermark = currentWatermark,
        retainFraction = config.fractionToRetainOnNodeCacheTrim
      )
      _ <- items.removeIfDiscard((_, cacheItem) => cacheItem.lastAccess <= nextWatermark)
      nextSize <- items.size
      _ <- watermark.set(nextWatermark)
    yield Some(
      TrimOutcome(
        previousWatermark = currentWatermark,
        nextWatermark = nextWatermark,
        previousSize = currentSize,
        nextSize = nextSize
      )
    )

  /** Calculate a new watermark that retains some fraction of the most current cache items.
    *
    * The cache will contain items with access times between the `purgeWatermark` and `now`.
    *
    * If the access times in the cache were uniform (which they almost certainly are not), then retaining some fraction
    * of the items based on access time will remove retain a corresponding fraction of the items in the cache. In the
    * case where the access times are strongly skewed towards `now`, this could potentially remove fewer (or zero)
    * items. Subsequent trim operations will cause the watermark to be moved forward again which will eventually cause
    * some or all of the cache to be trimmed.
    *
    * This is a weaker requirement for cache trimming since any one trim operation might not have the desired effect.
    * However, this method is designed to be simple to minimize computation within a transaction. This cache policy is
    * less strict since it allows the cache to expand beyond the desired capacity, but it will eventually be trimmed to
    * capacity.
    *
    * @param now
    *   The current clock time.
    * @param watermark
    *   The current oldest access time for any item in the cache. All items in the cache will have an access time that
    *   is greater than or equal to the purgeWatermark.
    * @return
    *   A new value for purgeWatermark that can be used to retain only some fraction of the newest cache items.
    */
  // visible for testing
  def moveWatermarkForward(now: AccessTime, watermark: AccessTime, retainFraction: Double): AccessTime =
    val intervals = now - watermark + 1
    val moveForwardBy = 1 max (intervals * retainFraction).toInt
    now min (watermark + moveForwardBy)

  override def startCurrentSnapshotTrimDaemon: UIO[Unit] =
    ZIO
      .when(config.currentSnapshotTrimFrequency != Duration.Zero) {
        currentSnapshotTrim
          .repeat(Schedule.spaced(config.currentSnapshotTrimFrequency))
          .catchAllCause { cause =>
            val operation = "node cache snapshot trim"
            ZIO.logCause(s"Unexpected failure in: $operation", cause) @@ LogAnnotations.operationAnnotation(operation)
          }
          .forkDaemon
      }
      .unit

  private def currentSnapshotTrim: UIO[Unit] =
    for
      now <- Clock.currentTime(MILLIS)
      outcome <- currentSnapshotTrimTransaction(now).commit
      _ <- ZIO.logInfo(s"Node cache snapshot trim") @@
        LogAnnotations.now(now) @@
        LogAnnotations.previousWatermark(outcome.previousWatermark) @@
        LogAnnotations.nextWatermark(outcome.nextWatermark) @@
        LogAnnotations.trimCount(outcome.trimCount)
    yield ()

  case class CurrentSnapshotTrimOutcome(previousWatermark: AccessTime, nextWatermark: AccessTime, trimCount: Int)

  private def currentSnapshotTrimTransaction(now: AccessTime): USTM[CurrentSnapshotTrimOutcome] =
    def shouldTrimCurrentSnapshot(cacheItem: CacheItem, nextWatermark: AccessTime): Boolean =
      cacheItem.lastAccess <= nextWatermark || cacheItem.current == null

    for
      currentWatermark <- watermark.get
      nextWatermark = moveWatermarkForward(
        now = now,
        watermark = currentWatermark,
        retainFraction = config.fractionOfSnapshotsToRetainOnSnapshotTrim
      )
      trimCount <- items.fold(0) { case (count, (_, cacheItem)) =>
        if shouldTrimCurrentSnapshot(cacheItem, nextWatermark) then count + 1 else count
      }
      _ <- items.transformValues(cacheItem =>
        // Purge the current node snapshot if the last access was before the retain time
        if shouldTrimCurrentSnapshot(cacheItem, nextWatermark) then cacheItem
        else cacheItem.copy(current = null)
      )
    yield CurrentSnapshotTrimOutcome(
      previousWatermark = currentWatermark,
      nextWatermark = nextWatermark,
      trimCount = trimCount
    )

object NodeCache:

  val layer: URLayer[AppConfig, NodeCache] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        now <- Clock.currentTime(MILLIS)
        oldest <- TRef.make(now).commit
        items <- TMap.empty[NodeId, CacheItem].commit
        nodeCache = NodeCacheLive(config.nodeCache, oldest, items)
        _ <- nodeCache.startCurrentSnapshotTrimDaemon
      yield nodeCache
    }
