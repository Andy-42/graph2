package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.stm.*
import zio.telemetry.opentelemetry.tracing.Tracing

trait MatcherSnapshotCache:
  def get(id: NodeId): NodeIO[NodeSnapshot]

  def fetchSnapshots(ids: Vector[NodeId]): NodeIO[Vector[NodeSnapshot]] = ZIO.foreachPar(ids)(get)

type SnapshotCacheKey = (NodeId, EventTime)

case class MatcherDataViewCacheLive(
    time: EventTime,
    nodeCache: MatcherNodeCache,
    snapshotCache: TMap[SnapshotCacheKey, NodeSnapshot],
    snapshotCacheInFlight: TSet[SnapshotCacheKey],
    tracing: Tracing
) extends MatcherSnapshotCache:

  import tracing.aspects.*

  private def acquireSnapshotCachePermit(id: => SnapshotCacheKey): UIO[SnapshotCacheKey] =
    STM
      .ifSTM(snapshotCacheInFlight.contains(id))(STM.retry, snapshotCacheInFlight.put(id).map(_ => id))
      .commit

  private def releaseSnapshotCachePermit(id: => SnapshotCacheKey): UIO[Unit] =
    snapshotCacheInFlight.delete(id).commit

  private def withSnapshotCachePermit(id: => SnapshotCacheKey): ZIO[Scope, Nothing, SnapshotCacheKey] =
    for
      _ <- tracing.setAttribute("id", id.toString)
      snapshotCacheKey <- ZIO.acquireRelease(acquireSnapshotCachePermit(id))(releaseSnapshotCachePermit(_))
    yield snapshotCacheKey

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
        _ <- withSnapshotCachePermit(key) @@ span("withSnapshotCachePermit")
        optionSnapshot <- snapshotCache.get(key).commit @@ span("snapshotCache.get")
        snapshot <- optionSnapshot.fold(getFromNodeCacheAndCacheSnapshot(id))(ZIO.succeed) @@ span(
          "getFromNodeCacheAndCacheSnapshot"
        )
      yield snapshot
    }

object MatcherSnapshotCache:
  def make(time: EventTime, nodeCache: MatcherNodeCache, tracing: Tracing): UIO[MatcherSnapshotCache] =
    for
      snapshotCache <- TMap.empty[SnapshotCacheKey, NodeSnapshot].commit
      snapshotCacheInFlight <- TSet.empty[SnapshotCacheKey].commit
    yield MatcherDataViewCacheLive(
      time = time,
      nodeCache = nodeCache,
      snapshotCache = snapshotCache,
      snapshotCacheInFlight = snapshotCacheInFlight,
      tracing = tracing
    )
