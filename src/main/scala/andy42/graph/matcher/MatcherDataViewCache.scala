package andy42.graph.matcher

import andy42.graph.services.Graph

import andy42.graph.model.*
import zio.stm.*
import zio.*

trait MatcherDataViewCache:
  def getSnapshot(id: NodeId, selector: SnapshotSelector): NodeIO[NodeSnapshot]
  def getHistory(id: NodeId): NodeIO[NodeHistory]

type SnapshotCacheKey = (NodeId, SnapshotSelector)

case class MatcherDataViewCacheLive(
    nodeCache: MatcherNodeCache,
    snapshotCache: TMap[SnapshotCacheKey, NodeSnapshot],
    historyCache: TMap[NodeId, NodeHistory],
    snapshotCacheInFlight: TSet[SnapshotCacheKey],
    historyCacheInFlight: TSet[NodeId],
    eventTime: EventTime
) extends MatcherDataViewCache {

  private def acquireSnapshotCachePermit(id: => SnapshotCacheKey): UIO[SnapshotCacheKey] =
    STM
      .ifSTM(snapshotCacheInFlight.contains(id))(STM.retry, snapshotCacheInFlight.put(id).map(_ => id))
      .commit

  private def releaseSnapshotCachePermit(id: => SnapshotCacheKey): UIO[Unit] =
    snapshotCacheInFlight.delete(id).commit

  private def withSnapshotCachePermit(id: => SnapshotCacheKey): ZIO[Scope, Nothing, SnapshotCacheKey] =
    ZIO.acquireRelease(acquireSnapshotCachePermit(id))(releaseSnapshotCachePermit(_))

  private def acquireHistoryCachePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(historyCacheInFlight.contains(id))(STM.retry, historyCacheInFlight.put(id).map(_ => id))
      .commit

  private def releaseHistoryCachePermit(id: => NodeId): UIO[Unit] =
    historyCacheInFlight.delete(id).commit

  // TODO: Could have separate permit schemes for history and snapshotstd
  private def withHistoryCachePermit(id: => NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquireHistoryCachePermit(id))(releaseHistoryCachePermit(_))

  private def getFromNodeCacheAndCacheSnapshot(id: NodeId, selector: SnapshotSelector): NodeIO[NodeSnapshot] =
    for
      node <- nodeCache.get(id)
      snapshot <- selector match {
        case SnapshotSelector.NodeCurrent  => node.current
        case SnapshotSelector.EventTime    => node.atTime(eventTime)
        case SnapshotSelector.AtTime(time) => node.atTime(time)
      }
      _ <- snapshotCache.put((id, selector), snapshot).commit
    yield snapshot

  private def getHistoryFromNodeCacheAndCacheHistory(id: NodeId): NodeIO[NodeHistory] =
    for
      node <- nodeCache.get(id)
      history <- node.history
      _ <- historyCache.put(id, history).commit
    yield history

  override def getSnapshot(
      id: NodeId,
      selector: SnapshotSelector
  ): NodeIO[NodeSnapshot] =
    ZIO.scoped {
      val key = (id, selector)
      for
        _ <- withSnapshotCachePermit(key)

        optionSnapshot <- snapshotCache.get(key).commit
        snapshot <- optionSnapshot.fold(getFromNodeCacheAndCacheSnapshot(id, selector))(ZIO.succeed)
      yield snapshot
    }

  override def getHistory(
      id: NodeId
  ): NodeIO[NodeHistory] =
    ZIO.scoped {
      for
        _ <- withHistoryCachePermit(id)

        optionHistory <- historyCache.get(id).commit
        history <- optionHistory.fold(getHistoryFromNodeCacheAndCacheHistory(id))(ZIO.succeed)
      yield history
    }
}

object MatcherDataViewCache:
  def make(nodeCache: MatcherNodeCache, eventTime: EventTime): UIO[MatcherDataViewCache] =
    for
      snapshotCache <- TMap.empty[SnapshotCacheKey, NodeSnapshot].commit
      historyCache <- TMap.empty[NodeId, NodeHistory].commit
      snapshotCacheInFlight <- TSet.empty[SnapshotCacheKey].commit
      historyCacheInFlight <- TSet.empty[NodeId].commit
    yield MatcherDataViewCacheLive(
      nodeCache = nodeCache,
      snapshotCache = snapshotCache,
      historyCache = historyCache,
      snapshotCacheInFlight = snapshotCacheInFlight,
      historyCacheInFlight = historyCacheInFlight,
      eventTime = eventTime
    )
