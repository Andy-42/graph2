package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.stm.*

trait MatcherSnapshotCache:
  def get(id: NodeId): NodeIO[NodeSnapshot]

  def fetchSnapshots(ids: Vector[NodeId]): NodeIO[Vector[NodeSnapshot]] = ZIO.foreachPar(ids)(get)

type SnapshotCacheKey = (NodeId, EventTime)

case class MatcherDataViewCacheLive(
    time: EventTime,
    nodeCache: MatcherNodeCache,
    snapshotCache: TMap[SnapshotCacheKey, NodeSnapshot],
    snapshotCacheInFlight: TSet[SnapshotCacheKey]
) extends MatcherSnapshotCache:

  private def acquireSnapshotCachePermit(id: => SnapshotCacheKey): UIO[SnapshotCacheKey] =
    STM
      .ifSTM(snapshotCacheInFlight.contains(id))(STM.retry, snapshotCacheInFlight.put(id).map(_ => id))
      .commit

  private def releaseSnapshotCachePermit(id: => SnapshotCacheKey): UIO[Unit] =
    snapshotCacheInFlight.delete(id).commit

  private def withSnapshotCachePermit(id: => SnapshotCacheKey): ZIO[Scope, Nothing, SnapshotCacheKey] =
    ZIO.acquireRelease(acquireSnapshotCachePermit(id))(releaseSnapshotCachePermit(_))

  private def getFromNodeCacheAndCacheSnapshot(id: NodeId): NodeIO[NodeSnapshot] =
    for
      node <- nodeCache.get(id)
      snapshot <- node.atTime(time)
      _ <- snapshotCache.put((id, time), snapshot).commit
    yield snapshot

  override def get(id: NodeId): NodeIO[NodeSnapshot] =
    ZIO.scoped {
      val key = (id, time)
      for
        _ <- withSnapshotCachePermit(key)
        optionSnapshot <- snapshotCache.get(key).commit
        snapshot <- optionSnapshot.fold(getFromNodeCacheAndCacheSnapshot(id))(ZIO.succeed)
      yield snapshot
    }

object MatcherSnapshotCache:
  def make(time: EventTime, nodeCache: MatcherNodeCache): UIO[MatcherSnapshotCache] =
    for
      snapshotCache <- TMap.empty[SnapshotCacheKey, NodeSnapshot].commit
      snapshotCacheInFlight <- TSet.empty[SnapshotCacheKey].commit
    yield MatcherDataViewCacheLive(
      time = time,
      nodeCache = nodeCache,
      snapshotCache = snapshotCache,
      snapshotCacheInFlight = snapshotCacheInFlight
    )
