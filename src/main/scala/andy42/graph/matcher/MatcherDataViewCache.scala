package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.stm.*

trait MatcherDataViewCache:
  def getSnapshot(id: NodeId, time: EventTime): NodeIO[NodeSnapshot]

type SnapshotCacheKey = (NodeId, EventTime)

case class MatcherDataViewCacheLive(
    nodeCache: MatcherNodeCache,
    snapshotCache: TMap[SnapshotCacheKey, NodeSnapshot],
    snapshotCacheInFlight: TSet[SnapshotCacheKey]
) extends MatcherDataViewCache {

  private def acquireSnapshotCachePermit(id: => SnapshotCacheKey): UIO[SnapshotCacheKey] =
    STM
      .ifSTM(snapshotCacheInFlight.contains(id))(STM.retry, snapshotCacheInFlight.put(id).map(_ => id))
      .commit

  private def releaseSnapshotCachePermit(id: => SnapshotCacheKey): UIO[Unit] =
    snapshotCacheInFlight.delete(id).commit

  private def withSnapshotCachePermit(id: => SnapshotCacheKey): ZIO[Scope, Nothing, SnapshotCacheKey] =
    ZIO.acquireRelease(acquireSnapshotCachePermit(id))(releaseSnapshotCachePermit(_))

  private def getFromNodeCacheAndCacheSnapshot(id: NodeId, time: EventTime): NodeIO[NodeSnapshot] =
    for
      node <- nodeCache.get(id)
      snapshot <- node.atTime(time)
      _ <- snapshotCache.put((id, time), snapshot).commit
    yield snapshot

  override def getSnapshot(
      id: NodeId,
      time: EventTime
  ): NodeIO[NodeSnapshot] =
    ZIO.scoped {
      val key = (id, time)
      for
        _ <- withSnapshotCachePermit(key)
        optionSnapshot <- snapshotCache.get(key).commit
        snapshot <- optionSnapshot.fold(getFromNodeCacheAndCacheSnapshot(id, time))(ZIO.succeed)
      yield snapshot
    }
}

object MatcherDataViewCache:
  def make(nodeCache: MatcherNodeCache): UIO[MatcherDataViewCache] =
    for
      snapshotCache <- TMap.empty[SnapshotCacheKey, NodeSnapshot].commit
      snapshotCacheInFlight <- TSet.empty[SnapshotCacheKey].commit
    yield MatcherDataViewCacheLive(
      nodeCache = nodeCache,
      snapshotCache = snapshotCache,
      snapshotCacheInFlight = snapshotCacheInFlight
    )
