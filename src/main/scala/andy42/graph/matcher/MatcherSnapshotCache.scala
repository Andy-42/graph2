package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.PersistenceFailure
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

trait MatcherSnapshotCache:
  def get(id: NodeId): NodeIO[NodeSnapshot]

  def fetchSnapshots(ids: Vector[NodeId]): NodeIO[Vector[NodeSnapshot]]

type NodeIOFailure = UnpackFailure | PersistenceFailure
type NodeIOSnapshotPromise = Promise[NodeIOFailure, NodeSnapshot]

case class MatcherSnapshotCacheLive(
    time: EventTime,
    nodeCache: MatcherNodeCache,
    snapshotCache: Ref[Map[NodeId, NodeIOSnapshotPromise]],
    tracing: Tracing
) extends MatcherSnapshotCache:

  import tracing.aspects.*

  override def get(id: NodeId): NodeIO[NodeSnapshot] =
    (
      for
        _ <- tracing.setAttribute("id", id.toString)

        newPromise <- Promise.make[NodeIOFailure, NodeSnapshot]

        promiseForThisId <- snapshotCache.modify(cache =>
          cache
            .get(id)
            .fold(newPromise -> cache.updated(id, newPromise))(existingPromise => existingPromise -> cache)
        )

        _ <- initiateFetch(id, newPromise).when(promiseForThisId eq newPromise)
        snapshot <- promiseForThisId.await
      yield snapshot
    ) @@ span("SnapshotCache.get")

  private def initiateFetch(id: NodeId, promise: NodeIOSnapshotPromise): NodeIO[Unit] =
    fetchSnapshot(id)
      .tapBoth(e => promise.fail(e), r => promise.succeed(r))
      .unit

  private def fetchSnapshot(id: NodeId): NodeIO[NodeSnapshot] =
    for
      node <- nodeCache.get(id)
      snapshot <- node.atTime(time)
    yield snapshot

  override def fetchSnapshots(ids: Vector[NodeId]): NodeIO[Vector[NodeSnapshot]] =
    for
      _ <- tracing.setAttribute("ids", ids.map(_.toString))
      snapshots <- ZIO.foreachPar(ids)(get)
    yield snapshots

object MatcherSnapshotCache:
  def make(
      time: EventTime,
      nodeCache: MatcherNodeCache,
      tracing: Tracing
  ): UIO[MatcherSnapshotCache] =
    for snapshotCache <- Ref.make(Map.empty[NodeId, NodeIOSnapshotPromise])
    yield MatcherSnapshotCacheLive(
      time = time,
      nodeCache = nodeCache,
      snapshotCache = snapshotCache,
      tracing = tracing
    )
