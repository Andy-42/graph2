package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

trait MatcherSnapshotCache:

  def prefetch(ids: Seq[NodeId]): NodeIO[Unit]

  def get(id: NodeId): NodeIO[NodeSnapshot]

type NodeIOSnapshotPromise = Promise[NodeIOFailure, NodeSnapshot]

case class MatcherSnapshotCacheLive(
    time: EventTime,
    graph: Graph,
    snapshotCache: Ref[Map[NodeId, NodeIOSnapshotPromise]],
    tracing: Tracing
) extends MatcherSnapshotCache:

  import tracing.aspects.*

  override def prefetch(ids: Seq[NodeId]): NodeIO[Unit] =
    (
      for
        promises <- ZIO.foreach(ids)(_ => Promise.make[NodeIOFailure, NodeSnapshot])

        idPromiseToBeFetched <- snapshotCache.modify { existingMap =>
          val idPromisesToBeFetched = ids.zip(promises).flatMap { (id, promise) =>
            existingMap.get(id).fold(Some(id -> promise))(_ => None)
          }

          idPromisesToBeFetched -> (existingMap ++ idPromisesToBeFetched)
        }

        _ <- ZIO.forkAllDiscard(idPromiseToBeFetched.map(initiateFetch))
      yield ()
    ).unless(ids.isEmpty).unit

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
      node <- graph.get(id)
      snapshot <- node.atTime(time)
    yield snapshot

object MatcherSnapshotCache:
  def make(
      time: EventTime,
      graph: Graph,
      nodes: Vector[Node],
      tracing: Tracing
  ): IO[NodeIOFailure, MatcherSnapshotCache] =
    for
      snapshotCacheMap <-
        ZIO
          .foreach(nodes) { node =>
            for
              snapshot <- node.atTime(time)
              promise <- Promise.make[NodeIOFailure, NodeSnapshot]
              _ <- promise.succeed(snapshot)
            yield node.id -> promise
          }
          .map(_.toMap)
      snapshotCache <- Ref.make(snapshotCacheMap)
    yield MatcherSnapshotCacheLive(
      time = time,
      graph = graph,
      snapshotCache = snapshotCache,
      tracing = tracing
    )
