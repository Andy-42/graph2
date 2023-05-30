package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.stm.*
import zio.telemetry.opentelemetry.tracing.Tracing

trait MatcherNodeCache:
  def get(id: NodeId): NodeIO[Node]

type NodeIONodePromise = Promise[NodeIOFailure, Node]

case class MatcherNodeCacheLive(
    graph: Graph,
    nodeCache: Ref[Map[NodeId, NodeIONodePromise]],
    tracing: Tracing
) extends MatcherNodeCache:

  import tracing.aspects.span

  override def get(
      id: NodeId
  ): NodeIO[Node] =
    (
      for
        _ <- tracing.setAttribute("id", id.toString)

        newPromise <- Promise.make[NodeIOFailure, Node]

        promiseForThisId <- nodeCache.modify(cache =>
          cache
            .get(id)
            .fold(newPromise -> cache.updated(id, newPromise))(existingPromise => existingPromise -> cache)
        )

        _ <- initiateFetch(id, newPromise).when(promiseForThisId eq newPromise)
        node <- promiseForThisId.await
      yield node
    ) @@ span("NodeCache.get")

  private def initiateFetch(id: NodeId, promise: NodeIONodePromise): NodeIO[Unit] =
    fetchNode(id)
      .tapBoth(e => promise.fail(e), r => promise.succeed(r))
      .unit

  private def fetchNode(id: NodeId): NodeIO[Node] =
    (
      for
        _ <- tracing.setAttribute("id", id.toString)

        node <- graph.get(id)
      yield node
    ) @@ span("Graph.get")

object MatcherNodeCache:
  def make(graph: Graph, nodes: Vector[Node], tracing: Tracing): UIO[MatcherNodeCache] =
    for cache <- Ref.make(Map.empty[NodeId, NodeIONodePromise])
    yield MatcherNodeCacheLive(graph = graph, nodeCache = cache, tracing = tracing)
