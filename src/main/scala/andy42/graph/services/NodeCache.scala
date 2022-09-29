package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.stm.*

import java.time.temporal.ChronoUnit.MILLIS
import com.sourcegraph.semanticdb_javac.Semanticdb.Access

trait NodeCache:
  def get(id: NodeId): IO[UnpackFailure, Option[Node]]
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
    oldest: TRef[AccessTime], // All items in the cache will have a lastAccess > oldest
    items: TMap[NodeId, CacheItem]
) extends NodeCache:

  override def get(id: NodeId): IO[UnpackFailure, Option[Node]] =
    for
      now <- Clock.currentTime(MILLIS)
      optionCacheItem <- getSTM(id, now).commit

      optionNode <- optionCacheItem.fold(ZIO.succeed(None)) { cacheItem =>
        if cacheItem.current != null then
          ZIO.succeed(
            Some(
              Node.replaceWithPackedHistory(
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
            Node.replaceWithHistory(
              id = id,
              history = history,
              current = snapshot,
              packed = cacheItem.packed
            )
          )
      }
    yield optionNode

  private def getSTM(
      id: NodeId,
      now: AccessTime
  ): USTM[Option[CacheItem]] =
    for optionItem <- items.updateWith(id)(_.map(_.copy(lastAccess = now)))
    yield optionItem

  override def put(node: Node): IO[UnpackFailure, Unit] =
    for
      now <- Clock.currentTime(MILLIS)

      current <- node.current

      optionOldest <- trimIfOverCapacity(now).commit
      _ <- optionOldest.fold(ZIO.unit)(oldest =>
        ZIO.logInfo("Node cache trimmed") @@ LogAnnotations.cacheItemRetainWatermark(oldest)
      )

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

  private def trimIfOverCapacity(now: AccessTime): USTM[Option[AccessTime]] =
    ZSTM.ifSTM(items.size.map(_ > config.capacity))(
      onTrue = trim(now),
      onFalse = ZSTM.succeed(None)
    )

  private def trimSnapshot(now: AccessTime): USTM[AccessTime] =
    for
      oldest <- oldest.get
      retain = lastTimeToRetainSnapshot(oldest, now)
      _ <- items.transformValues(cacheItem =>
        // Purge the current node snapshot if the last access was before the retain time
        if cacheItem.lastAccess >= retain || cacheItem.current == null then cacheItem
        else cacheItem.copy(current = null)
      )
    yield retain

  def snapshotTrim: UIO[Unit] =
    for
      now <- Clock.currentTime(MILLIS)
      retain <- trimSnapshot(now).commit
      _ <- ZIO.logInfo("Node cache snapshot trim") @@ LogAnnotations.snapshotRetainWatermark(retain)
    yield ()

  private def lastTimeToRetainSnapshot(oldest: AccessTime, now: AccessTime): AccessTime =
    now - Math.ceil((now - oldest + 1) * config.fractionOfSnapshotsToRetainOnSnapshotPurge).toLong

  override def startSnapshotTrim: UIO[Unit] =
    snapshotTrim
      .repeat(Schedule.spaced(config.snapshotPurgeFrequency))
      .catchAllCause(cause =>
        val operation = "node cache snapshot trim"
        ZIO.logCause(s"Unexpected failure in: $operation", cause) @@ LogAnnotations.operationAnnotation(operation)
      )
      .forkDaemon *> ZIO.unit

  private def trim(now: AccessTime): USTM[Some[AccessTime]] =
    for
      currentOldest <- oldest.get
      newOldest = lastTimeToPurge(currentOldest, now)
      _ <- items.removeIfDiscard((_, cacheItem) => cacheItem.lastAccess < newOldest)
      _ <- oldest.set(newOldest)
    yield Some(newOldest)

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
        now <- Clock.currentTime(MILLIS)
        items <- TMap.empty[NodeId, CacheItem].commit
        oldest <- TRef.make(now - 1).commit

        nodeCache = NodeCacheLive(config, oldest, items)
        _ <- nodeCache.startSnapshotTrim
      yield nodeCache
    }
