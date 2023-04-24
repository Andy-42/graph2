package andy42.graph.matcher

import andy42.graph.model.*
import andy42.graph.services.Graph
import zio.*
import zio.stm.*

trait MatcherNodeCache:
  def get(id: NodeId): NodeIO[Node]

case class MatcherNodeCacheLive(
    graph: Graph,
    cache: TMap[NodeId, Node],
    inFlight: TSet[NodeId]
) extends MatcherNodeCache:

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  private def withNodePermit(id: => NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  private def getFromGraphAndCache(id: NodeId): NodeIO[Node] =
    for
      node <- graph.get(id)
      _ <- cache.put(id, node).commit
    yield node

  override def get(
      id: NodeId
  ): NodeIO[Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        optionNode <- cache.get(id).commit
        node <- optionNode.fold(getFromGraphAndCache(id))(ZIO.succeed)
      yield node
    }

object MatcherNodeCache:
  def make(graph: Graph, nodes: Vector[Node]): UIO[MatcherNodeCache] =
    for
      cache <- TMap.fromIterable[NodeId, Node](nodes.map(node => node.id -> node)).commit
      inFlight <- TSet.empty[NodeId].commit
    yield MatcherNodeCacheLive(
      graph = graph,
      cache = cache,
      inFlight = inFlight
    )
