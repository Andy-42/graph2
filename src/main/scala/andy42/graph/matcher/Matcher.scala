package andy42.graph.matcher

import andy42.graph.matcher.NodeSpec
import andy42.graph.model.*
import andy42.graph.services.{Graph, NodeMutationOutput, PersistenceFailure}
import zio.*

import scala.annotation.tailrec

type NodeSpecName = String
type SubgraphMatch = Map[NodeSpecName, NodeId]

trait Matcher:

  /** Matching is done as of a particular point in time. */
  val time: EventTime

  def matchNodes(ids: Seq[NodeId]): NodeIO[List[SubgraphMatch]]

// TODO: After matching, if there are any nodes in the cache where the current > time
// then we need to match those nodes at those times, otherwise, it is possible to miss matches.
// The NodeCache can be reused, but the MatcherDataViewCache needs to be re-created.

case class MatcherLive(time: EventTime, subgraphSpec: SubgraphSpec, cache: MatcherDataViewCache) extends Matcher:

  /** Match a set of nodes against a SubgraphSpec.
    *
    * Each node-spec pair is matched in parallel.
    *
    * @param ids
    *   The nodes that are to be matched to the Subgraph spec.
    * @return
    *   A list of subgraph matches, where each entry defined a match between a NodeSpec and a Node at that time. The
    *   matches are not necessarily unique, but are complete.
    */
  override def matchNodes(ids: Seq[NodeId]): NodeIO[List[SubgraphMatch]] =
    for
      matches <- ZIO.foreachPar(nodeAndSpecCrossProduct(ids))((id, nodeSpec) =>
        matchNode(id, nodeSpec, subgraphMatch = Map.empty)
      )
      postFilteredMatches <- subgraphSpec.filter.fold(ZIO.succeed(matches.flatten)) { postFilter =>
        ZIO.filter(matches.flatten)(subgraphMatch =>
          postFilter.p(using SnapshotProviderLive(time, cache, subgraphMatch))
        )
      }
    yield postFilteredMatches

  private def nodeAndSpecCrossProduct(nodeIds: Seq[NodeId]): List[(NodeId, NodeSpec)] =
    for
      nodeId <- nodeIds.toList
      nodeSpec <- subgraphSpec.nameToNodeSpec.values
    yield (nodeId, nodeSpec)

  /** Match a node by traversing the graph from a node starting point.
    *
    * @param id
    *   The id of the graph node being matched.
    * @param nodeSpec
    *   The spec that the node is being matched against
    * @param subgraphMatch
    *   All the matched nodes that have been matched so far in this graph traversal.
    * @return
    *   All the matches accumulated so far for this node.
    */
  private def matchNode(
      id: NodeId,
      nodeSpec: NodeSpec,
      subgraphMatch: SubgraphMatch
  ): NodeIO[List[SubgraphMatch]] =
    for
      snapshot <- cache.getSnapshot(id, time)
      matches <-
        if shallowMatch(nodeSpec)(using snapshot) then deepMatch(id, snapshot, nodeSpec, subgraphMatch)
        else ZIO.succeed(List.empty)
    yield matches

  /** Test the local state of the node to determine if there is at least one possible match */
  private def shallowMatch(nodeSpec: NodeSpec)(using snapshot: NodeSnapshot): Boolean =
    nodeSpec.allPredicatesMatch && subgraphSpec.outgoingEdges(nodeSpec.name).forall(someShallowMatchExists)

  /** Find all the possible matches for the part of the subgraph reachable from this node.
    *
    * If this node is already in the subgraph match so far, then the edges have looped back to some previously-matched
    * point.
    *
    * @return
    *   A SubgraphMatch that includes matches for all nodes that are reachable from this node.
    */
  private def deepMatch(
      id: NodeId,
      snapshot: NodeSnapshot,
      nodeSpec: NodeSpec,
      subgraphMatchSoFar: SubgraphMatch
  ): NodeIO[List[SubgraphMatch]] =
    if subgraphMatchSoFar.contains(nodeSpec.name) then ZIO.succeed(List(subgraphMatchSoFar))
    else eachReferencedNodeMatches(nodeSpec, subgraphMatchSoFar.updated(nodeSpec.name, id))(using snapshot)

  /** Match the outgoing edges.
    *
    * Since there are possibly multiple combinations of edges that could match, each possible combination is tried, so
    * it is possible that multiple `SubgraphMatch`es can be returned for one match. Each combination of subgraph matches
    * are tested in parallel.
    */
  private def eachReferencedNodeMatches(nodeSpec: NodeSpec, matchingNodes: SubgraphMatch)(using
      snapshot: NodeSnapshot
  ): NodeIO[List[SubgraphMatch]] =
    shallowMatchesForEachEdgeSpec(nodeSpec)
      .fold(ZIO.succeed(List.empty))(groupsOfEdgesThatShouldMatch =>
        for x <- ZIO.foreachPar(groupsOfEdgesThatShouldMatch)(matchEachEdge(_, matchingNodes))
        yield x.flatten
      )

  /** Generate combinations.
    *
    * This is the method from StackOverflow via Micaela Martino's version (re-formatted)
    * https://stackoverflow.com/questions/23425930/generating-all-possible-combinations-from-a-listlistint-in-scala
    *
    * @param xs
    *   A list of lists of A
    * @return
    *   A list of all the combinations that can be generated using one value from each of the lists.
    */
  private def generator[A](xs: List[List[A]]): List[List[A]] =
    xs.foldRight(List(List.empty[A])) { (next, combinations) =>
      for
        a <- next
        as <- combinations
      yield a +: as
    }

  /** All the edge combinations that could possibly match.
    *
    * The first step is to calculate all the matches in a cross-product of the edges and the edge specs. An edge spec
    * could be matched by multiple edges, so we have to calculate all the possible combinations.
    */
  private def shallowMatchesForEachEdgeSpec(nodeSpec: NodeSpec)(using
      snapshot: NodeSnapshot
  ): Option[List[List[(NodeSpecName, NodeId)]]] =

    @tailrec def accumulate(
        outgoing: List[EdgeSpec] = subgraphSpec.outgoingEdges(nodeSpec.name),
        r: List[List[(NodeSpecName, NodeId)]] = List.empty
    ): Option[List[List[(NodeSpecName, NodeId)]]] =
      outgoing match {
        case edgeSpec :: tail =>
          val farNodeSpecName = edgeSpec.direction.to.name
          val possiblyMatchingEdges = snapshot.edges.toList.collect {
            case edge if edgeSpec.isShallowMatch(edge) => farNodeSpecName -> edge.other
          }

          if (possiblyMatchingEdges.isEmpty) None
          else accumulate(outgoing = tail, r = r :+ possiblyMatchingEdges)

        case _ => Some(r)
      }

    accumulate().map(generator)

  /** Determine whether a list of NodeSpec and NodeIds match. The match is done recursively through all nodes that are
    * matchable through the given NodeIds.
    *
    * Each edge is matched in parallel.
    *
    * @param targets
    *   A list of NodeSpecName
    * @param subgraphMatchSoFar
    *   Any nodes that have been matched so far. This will include the node currently being matched.
    * @return
    *   All possible subgraph matches that can be accumulated by matching from the given edge matches.
    */
  private def matchEachEdge(
      targets: List[(NodeSpecName, NodeId)],
      subgraphMatchSoFar: SubgraphMatch
  ): NodeIO[List[SubgraphMatch]] =
    for x <- ZIO.foreachPar(targets)((farNodeSpecName, farId) =>
        matchNode(farId, subgraphSpec.nameToNodeSpec(farNodeSpecName), subgraphMatchSoFar)
      )
    yield x.flatten

object Matcher:

  def make(time: EventTime, graph: Graph, subgraphSpec: SubgraphSpec, nodes: Vector[Node]): UIO[Matcher] =
    for
      nodeCache <- MatcherNodeCache.make(graph, nodes)
      dataViewCache <- MatcherDataViewCache.make(nodeCache)
    yield MatcherLive(time, subgraphSpec, dataViewCache)
