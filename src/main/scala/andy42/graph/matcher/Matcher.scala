package andy42.graph.matcher

import andy42.graph.matcher.NodeSpec
import andy42.graph.model.*
import andy42.graph.services.{Graph, NodeMutationOutput, PersistenceFailure}
import zio.*

type NodeMatches = Map[String, NodeId]

trait Matcher:

  val time: EventTime

  def matchNodes(ids: Seq[NodeId]): NodeIO[List[NodeMatches]]

case class MatcherLive(time: EventTime, subgraphSpec: SubgraphSpec, cache: MatcherDataViewCache) extends Matcher:

  override def matchNodes(ids: Seq[NodeId]): NodeIO[List[NodeMatches]] =
    ZIO.foldLeft(nodeAndSpecCrossProduct(ids.toList))(List.empty) { case (r, (id, nodeSpec)) =>
      for x <- matchNodeToNodeSpec(id, nodeSpec, subgraphMatch = Map.empty)
      yield x.fold(r)(_ :: r)
    }

  def nodeAndSpecCrossProduct(nodeIds: List[NodeId]): List[(NodeId, NodeSpec)] =
    for
      nodeId <- nodeIds
      nodeSpec <- subgraphSpec.nameToNodeSpec.values
    yield (nodeId, nodeSpec)

  def matchNodeToNodeSpec(
      id: NodeId,
      nodeSpec: NodeSpec,
      subgraphMatch: NodeMatches
  ): NodeIO[Option[NodeMatches]] =
    ZIO.ifZIO(localPredicatesMatch(id, nodeSpec))(
      onTrue = deepMatch(id, nodeSpec, subgraphMatch),
      onFalse = ZIO.none
    )

  /** Test the local state of the node to determine if there is at least one possible match */
  def localPredicatesMatch(nodeSpec: NodeSpec)(using snapshot: NodeSnapshot): Boolean =
    nodeSpec.allPredicatesMatch && subgraphSpec.outgoingEdges(nodeSpec.name).forall(someLocalMatchExists)

  def localPredicatesMatch(id: NodeId, nodeSpec: NodeSpec): NodeIO[Boolean] =
    for snapshot <- cache.getSnapshot(id, time)
    yield localPredicatesMatch(nodeSpec)(using snapshot)

  /** All the edge combinations that could possibly match. This assumes that testLocal has previously be tested and
    * returned true
    */
  def localMatchesForEachEdgeSpec(
      nodeSpec: NodeSpec
  )(using snapshot: NodeSnapshot): List[(String, List[NodeId])] =
    subgraphSpec
      .outgoingEdges(nodeSpec.name)
      .map { edgeSpec =>
        val farNodeSpecName = edgeSpec.direction.to.name
        val farNodeIds = snapshot.edges.toList.collect { case edge if edgeSpec.isLocalMatch(edge) => edge.other }
        farNodeSpecName -> farNodeIds
      }

  /** For a node that matches locally, do a deep test to check that all referenced */
  def deepMatch(
      id: NodeId,
      nodeSpec: NodeSpec,
      subgraphMatch: NodeMatches
  ): NodeIO[Option[NodeMatches]] =
    if subgraphMatch.contains(nodeSpec.name) then ZIO.some(subgraphMatch)
    else
      for
        snapshot <- cache.getSnapshot(id, time)
        finalResolution <- eachReferencedNodeMatches(id, nodeSpec, subgraphMatch)(using snapshot)
      yield finalResolution

  def eachReferencedNodeMatches(id: NodeId, nodeSpec: NodeSpec, subgraphMatch: NodeMatches)(using
                                                                                            snapshot: NodeSnapshot
  ): NodeIO[Option[NodeMatches]] =
    val allMatches = localMatchesForEachEdgeSpec(nodeSpec) // TODO: short-circuit
    if allMatches.exists(_._2.isEmpty) then ZIO.none
    else
      // PUNT: Only consider the first combination - TODO: Test each combination!
      val targets: List[(String, NodeId)] = allMatches.map((k, l) => k -> l.head)

      // TODO: Why does foldLeft require type parameters to compile?
      ZIO.foldLeft[Any, UnpackFailure | PersistenceFailure, Option[NodeMatches], (String, NodeId)](targets)(
        Some(subgraphMatch.updated(nodeSpec.name, id))
      ) {
        case (Some(subgraphMatch), (name, id)) =>
          matchNodeToNodeSpec(id, subgraphSpec.nameToNodeSpec(name), subgraphMatch)
        case _ =>
          ZIO.none
      }

object Matcher:

  def make(time: EventTime, graph: Graph, subgraphSpec: SubgraphSpec, nodes: Vector[Node]): UIO[Matcher] =
    for
      nodeCache <- MatcherNodeCache.make(graph, nodes)
      dataViewCache <- MatcherDataViewCache.make(nodeCache)
    yield MatcherLive(time, subgraphSpec, dataViewCache)
