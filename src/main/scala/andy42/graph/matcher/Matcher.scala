package andy42.graph.matcher

import andy42.graph.matcher.NodeSpec
import andy42.graph.model.*
import andy42.graph.services.{Graph, NodeMutationOutput, PersistenceFailure}
import zio.*

type MatchingNodes = Map[String, NodeId]

trait Matcher:

  val time: EventTime

  // TODO: After matching, if there are any nodes in the cache where the current > time
  // then we need to run SQE for all those nodes/times, otherwise, it is possible to miss matches.
  // The NodeCache can be reused, but the MatcherDataViewCache needs to be re-created.

  def matchNodes(ids: Seq[NodeId]): NodeIO[List[MatchingNodes]]

case class MatcherLive(time: EventTime, subgraphSpec: SubgraphSpec, cache: MatcherDataViewCache) extends Matcher:

  override def matchNodes(ids: Seq[NodeId]): NodeIO[List[MatchingNodes]] =
    ZIO.foldLeft(nodeAndSpecCrossProduct(ids.toList))(List.empty) { case (matchingNodes, (id, nodeSpec)) =>
      for maybeMatchingNodes <- matchNode(id, nodeSpec, nodeMatchesSoFar = Map.empty)
      yield maybeMatchingNodes.fold(matchingNodes)(_ :: matchingNodes)
    }

  private def nodeAndSpecCrossProduct(nodeIds: List[NodeId]): List[(NodeId, NodeSpec)] =
    for
      nodeId <- nodeIds
      nodeSpec <- subgraphSpec.nameToNodeSpec.values
    yield (nodeId, nodeSpec)

  private def matchNode(
      id: NodeId,
      nodeSpec: NodeSpec,
      nodeMatchesSoFar: MatchingNodes
  ): NodeIO[Option[MatchingNodes]] =
    ZIO.ifZIO(shallowMatch(id, nodeSpec))(
      onTrue = deepMatch(id, nodeSpec, nodeMatchesSoFar),
      onFalse = ZIO.none
    )

  /** Test the local state of the node to determine if there is at least one possible match */
  private def shallowMatch(nodeSpec: NodeSpec)(using snapshot: NodeSnapshot): Boolean =
    nodeSpec.allPredicatesMatch && subgraphSpec.outgoingEdges(nodeSpec.name).forall(someShallowMatchExists)

  private def shallowMatch(id: NodeId, nodeSpec: NodeSpec): NodeIO[Boolean] =
    for snapshot <- cache.getSnapshot(id, time)
    yield shallowMatch(nodeSpec)(using snapshot)

  /** For a node that matches locally, do a deep test to check that all referenced */
  private def deepMatch(
      id: NodeId,
      nodeSpec: NodeSpec,
      nodeMatchesSoFar: MatchingNodes
  ): NodeIO[Option[MatchingNodes]] =
    if nodeMatchesSoFar.contains(nodeSpec.name) then ZIO.some(nodeMatchesSoFar)
    else
      for
        snapshot <- cache.getSnapshot(id, time)
        finalResolution <- eachReferencedNodeMatches(id, nodeSpec, nodeMatchesSoFar)(using snapshot)
      yield finalResolution

  /** All the edge combinations that could possibly match. This assumes that testLocal has previously be tested and
    * returned true
    */
  private def shallowMatchesForEachEdgeSpec(
      nodeSpec: NodeSpec
  )(using snapshot: NodeSnapshot): List[(String, List[NodeId])] =
    subgraphSpec
      .outgoingEdges(nodeSpec.name)
      .map { edgeSpec =>
        val farNodeSpecName = edgeSpec.direction.to.name
        val farNodeIds = snapshot.edges.toList.collect { case edge if edgeSpec.isShallowMatch(edge) => edge.other }
        farNodeSpecName -> farNodeIds
      }

  private def eachReferencedNodeMatches(id: NodeId, nodeSpec: NodeSpec, subgraphMatch: MatchingNodes)(using
      snapshot: NodeSnapshot
  ): NodeIO[Option[MatchingNodes]] =
    val allMatches = shallowMatchesForEachEdgeSpec(nodeSpec) // TODO: short-circuit
    if allMatches.exists(_._2.isEmpty) then ZIO.none
    else
      // PUNT: Only consider the first combination - TODO: Test each combination!
      val targets: List[(String, NodeId)] = allMatches.map((k, l) => k -> l.head)

      ZIO.foldLeft[Any, UnpackFailure | PersistenceFailure, Option[MatchingNodes], (String, NodeId)](targets)(
        Some(subgraphMatch.updated(nodeSpec.name, id))
      ) {
        case (Some(subgraphMatch), (name, id)) =>
          matchNode(id, subgraphSpec.nameToNodeSpec(name), subgraphMatch)
        case _ => ZIO.none
      }

object Matcher:

  def make(time: EventTime, graph: Graph, subgraphSpec: SubgraphSpec, nodes: Vector[Node]): UIO[Matcher] =
    for
      nodeCache <- MatcherNodeCache.make(graph, nodes)
      dataViewCache <- MatcherDataViewCache.make(nodeCache)
    yield MatcherLive(time, subgraphSpec, dataViewCache)
