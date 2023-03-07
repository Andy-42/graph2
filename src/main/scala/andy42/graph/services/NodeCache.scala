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

      optionCacheItem <- items.get(id).commit

      _ <- optionCacheItem.fold(ZIO.unit)(item =>
        // A hot node item might be referenced many times in one millisecond, so only update if lastAccess would change
        if item.lastAccess < now then
          val e = items.put(id, item.copy(lastAccess = now)).commit
          if config.forkOnUpdateAccessTime then e.fork else e // The safety of forking here is questionable
        else ZIO.unit
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

      _ <- trimIfOverCapacity(now) // TODO: fork
    yield ()

  // TODO: Avoid a cascade of trims by capturing the watermark and comparing before trim  

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
      nextWatermark = moveWatermarkForward(now = now, watermark = currentWatermark)
      _ <- items.removeIfDiscard((_, cacheItem) => cacheItem.lastAccess <= nextWatermark)
      _ <- watermark.set(nextWatermark)
    yield Some(nextWatermark)

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
    * @param purgeWatermark
    *   The current oldest access time for any item in the cache. All items in the cache will have an access time that
    *   is greater than or equal to the purgeWatermark.
    * @param fractionToRetain
    *   When trimming the cache, we retain only those items that have an access time that are in the fraction of items
    *   at the end of the internal between `now` and `purgeWatermark`.
    * @return
    *   A new value for purgeWatermark that can be used to retain only some fraction of the newest cache items.
    */
  // visible for testing  
  def moveWatermarkForward(now: AccessTime, watermark: AccessTime): AccessTime =
    val intervals = now - watermark + 1
    val moveForwardBy = 1 max (intervals * config.fractionToRetainOnTrim).toInt
    now min (watermark + moveForwardBy)

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
      _ <- ZIO.logInfo(s"Node cache snapshot trim") @@ LogAnnotations.snapshotRetainWatermark(retain)
    yield ()

  private def snapshotTrimTransaction(now: AccessTime): USTM[AccessTime] =
    for
      currentWatermark <- watermark.get
      snapshotWatermark = moveWatermarkForward(currentWatermark, now)
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
        oldest <- TRef.make(now).commit
        items <- TMap.empty[NodeId, CacheItem].commit
        nodeCache = NodeCacheLive(config, oldest, items)
        _ <- nodeCache.startSnapshotTrimDaemon
      yield nodeCache
    }
