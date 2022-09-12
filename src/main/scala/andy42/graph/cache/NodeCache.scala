package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.stm._

import java.time.temporal.ChronoUnit.MILLIS

trait NodeCache:
  def get(id: NodeId): UIO[Option[Node]]
  def put(node: Node): IO[UnpackFailure, Unit]

  def startSnapshotTrim: UIO[Unit]

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
    clock: Clock,
    oldest: TRef[AccessTime], // All items in the cache will have a lastAccess > oldest
    items: TMap[NodeId, CacheItem]
) extends NodeCache:

  override def get(id: NodeId): UIO[Option[Node]] =
    for
      now <- clock.currentTime(MILLIS)
      optionNode <- getSTM(id, now).commit

      // TODO: Make getSTM return a CacheItem, and the 
    yield optionNode

  private def getSTM(
      id: NodeId,
      now: AccessTime
  ): USTM[Option[Node]] =
    for optionItem <- items.updateWith(id)(_.map(_.copy(lastAccess = now)))
    yield optionItem.map( cacheItem => Node.fromCacheItem(id, cacheItem))

  override def put(node: Node): IO[UnpackFailure, Unit] =
    for
      now <- clock.currentTime(MILLIS)

      current <- node.current

      _ <- trimIfOverCapacity(now).commit
      _ <- putSTM(node, now, current).commit
    yield ()

  private def putSTM(
      node: Node,
      now: AccessTime,
      current: NodeSnapshot
  ): USTM[Unit] =
    for _ <- items.put(
        node.id,
        CacheItem(
          version = node.version,
          lastTime = node.lastTime,
          lastSequence = node.lastSequence,
          current = current,
          packed = node.packedHistory,
          lastAccess = now
        )
      )
    yield ()

  private def trimIfOverCapacity(now: AccessTime): USTM[Unit] =
    ZSTM.ifSTM(items.size.map(_ > config.capacity))(
      onTrue = trim(now),
      onFalse = ZSTM.unit
    )

  private def trimSnapshot(now: AccessTime): USTM[Unit] =
    for
      oldest <- oldest.get
      retain = lastTimeToRetainSnapshot(oldest, now)
      _ <- items.transformValues(cacheItem =>
        // Purge the edges and properties.
        // This could potentially purge only one or the other, or could also be based on the size of properties/edges
        if cacheItem.lastAccess > retain || cacheItem.current == null then cacheItem
        else cacheItem.copy(current = null)
      )
    yield ()

  // TODO: Logging  
  def snapshotTrim: UIO[Unit] =
    for
      now <- clock.currentTime(MILLIS)
      _ <- trimSnapshot(now).commit
    yield ()

  private def lastTimeToRetainSnapshot(oldest: AccessTime, now: AccessTime): AccessTime =
    now - Math.ceil((now - oldest + 1) * config.fractionOfSnapshotsToRetainOnSnapshotPurge).toLong

  override def startSnapshotTrim: UIO[Unit] =
    snapshotTrim.repeat(Schedule.spaced(config.snapshotPurgeFrequency))
    .fork *> ZIO.unit // TODO: Handle fiber death

  // TODO: Logging  
  private def trim(now: AccessTime): USTM[Unit] =
    for
      currentOldest <- oldest.get
      newOldest = lastTimeToPurge(currentOldest, now)
      _ <- items.removeIfDiscard((_, cacheItem) => cacheItem.lastAccess < newOldest)
      _ <- oldest.set(newOldest)
    yield ()

  /** The time would retain the configured fraction of the cache if we removed all cache items with an access time less
    * than that time.
    *
    * This algorithm assumes that the distribution of access times in the cache is uniform, but the reality is that it
    * will skew to the more recent (gamma?).
    *
    * @param oldest
    *   The current oldest access time (all cache items have a more recent access time)
    * @param now
    * @return
    *   A new value for oldest that can be used to remove some fraction of the oldest cache items.
    */
  private def lastTimeToPurge(oldest: AccessTime, now: AccessTime): AccessTime =
    now - Math.ceil((now - oldest + 1) * config.fractionToRetainOnTrim).toLong

object NodeCache:

  val layer: URLayer[NodeCacheConfig & Clock, NodeCache] =
    ZLayer {
      for
        config <- ZIO.service[NodeCacheConfig]
        clock <- ZIO.service[Clock] // TODO: Make this a field?
        now <- clock.currentTime(MILLIS)
        items <- TMap.empty[NodeId, CacheItem].commit
        oldest <- TRef.make(now - 1).commit
    
        nodeCache = NodeCacheLive(
          config = config,
          clock = clock,
          oldest = oldest,
          items = items
        )
        _ <- nodeCache.startSnapshotTrim
      yield nodeCache 
    }
