package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.stm.*

import java.time.temporal.ChronoUnit.MILLIS

trait NodeCache:
  def get(id: NodeId): IO[UnpackFailure, Option[Node]]
  def put(node: Node): IO[UnpackFailure, Unit]

  def startSnapshotTrimDaemon: UIO[Unit]

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
      optionCacheItem <- items
        .updateWith(id)(_.map(_.copy(lastAccess = now)))
        .commit

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

      _ <- trimIfOverCapacity(now) // TODO: fork
    yield ()

  private def trimIfOverCapacity(now: AccessTime): UIO[Unit] =
    for
      optionOldest <- ZSTM
        .ifSTM(items.size.map(_ > config.capacity))(trim(now), ZSTM.succeed(None))
        .commit

      _ <- optionOldest.fold(ZIO.unit)(oldest =>
        ZIO.logInfo("Node cache trimmed") @@ LogAnnotations.cacheItemRetainWatermark(oldest)
      )
    yield ()

  private def trim(now: AccessTime): USTM[Some[AccessTime]] =
    for
      currentWatermark <- watermark.get
      nextWatermark = moveWatermarkForward(currentWatermark, now, config.fractionToRetainOnTrim)
      _ <- items.removeIfDiscard((_, cacheItem) => cacheItem.lastAccess < nextWatermark)
      _ <- watermark.set(nextWatermark)
    yield Some(nextWatermark)

  /** The time would retain the configured fraction of the cache if we removed all cache items with an access time less
    * than that time.
    *
    * This algorithm assumes that the distribution of access times in the cache is uniform, but the reality is that it
    * will skew to the more recent (gamma?).
    *
    * @param oldest // TODO: Fix comment
    *   The current oldest access time (all cache items have a more recent access time)
    * @param now // TODO: Fix comment
    * @return
    *   A new value for oldest that can be used to remove some fraction of the oldest cache items.
    */
  private def moveWatermarkForward(now: AccessTime, purgeWatermark: AccessTime, fractionToRetain: Double): AccessTime =
    val intervals = now - purgeWatermark + 1
    val moveForwardBy = 1 max (intervals * config.fractionToRetainOnTrim).toInt
    purgeWatermark + moveForwardBy

  override def startSnapshotTrimDaemon: UIO[Unit] =
    snapshotTrim
      .repeat(Schedule.spaced(config.snapshotPurgeFrequency))
      .catchAllCause { cause =>
        val operation = "node cache snapshot trim"
        ZIO.logCause(s"Unexpected failure in: $operation", cause) @@ LogAnnotations.operationAnnotation(operation)
      }
      .forkDaemon *> ZIO.unit

  private def snapshotTrim: UIO[Unit] =
    for
      now <- Clock.currentTime(MILLIS)
      retain <- snapshotTrimTransaction(now).commit
      _ <- ZIO.logInfo("Node cache snapshot trim") @@ LogAnnotations.snapshotRetainWatermark(retain)
    yield ()

  private def snapshotTrimTransaction(now: AccessTime): USTM[AccessTime] =
    for
      currentWatermark <- watermark.get
      snapshotWatermark = moveWatermarkForward(currentWatermark, now, config.fractionOfSnapshotsToRetainOnSnapshotPurge)
      _ <- items.transformValues(cacheItem =>
        // Purge the current node snapshot if the last access was before the retain time
        if cacheItem.lastAccess >= snapshotWatermark || cacheItem.current == null then cacheItem
        else cacheItem.copy(current = null)
      )
    yield snapshotWatermark

object NodeCache:

  val layer: URLayer[NodeCacheConfig, NodeCache] =
    ZLayer {
      for
        config <- ZIO.service[NodeCacheConfig]
        now <- Clock.currentTime(MILLIS)
        oldest <- TRef.make(now - 1).commit
        items <- TMap.empty[NodeId, CacheItem].commit
        nodeCache = NodeCacheLive(config, oldest, items)
        _ <- nodeCache.startSnapshotTrimDaemon
      yield nodeCache
    }
