package andy42.graph.matcher

import andy42.graph.matcher.EdgeSpecs.{directedEdge, undirectedEdge}
import andy42.graph.model.{NodeId, *}
import andy42.graph.model.Generators.*
import zio.test.*

object MatcherSpec extends ZIOSpecDefault:

  /** All tests in this fixture use a test at a single event time, the value of which doesn't matter in these tests. */
  val time: EventTime = 42

  extension (subgraphSpec: SubgraphSpec)
    /** Match `subgraphSpec` against the nodes in a graph, starting matching at the given starting points.
      * @param graphNodes
      *   A collection of nodes that contains at least all reachable nodes in `matchStartingPoints`. In this test
      *   fixture, the Graph service is not used, so all nodes must be available from the cache. Since the Graph service
      *   is not used, it is up to the caller to construct a well-formed graph contents (e.g., half-edge consistency).
      * @param matchStartingPoints
      *   Matching will use these NodeIds as starting points in matching. All these nodes must be in the cache, but
      *   there is no requirement that all nodes in the cache are starting points for matching.
      * @return
      *   All the matches for this subgraph spec starting matching at the given nodes.
      */
    def matchTo(
        graphNodes: Vector[Node],
        matchStartingAt: Vector[NodeId]
    ): NodeIO[List[SubgraphMatch]] =
      for
        matcher <- Matcher.make(
          time = time,
          graph = UnimplementedGraph(), // unused - all nodes will be fetched from the cache
          subgraphSpec = subgraphSpec,
          nodes = graphNodes
        )
        subgraphMatches <- matcher.matchNodes(matchStartingAt)
      yield subgraphMatches

  def makeNode(id: NodeId, events: Event*): Node =
    Node.fromHistory(
      id = id,
      history = Vector(
        EventsAtTime(
          time = time,
          sequence = 0,
          events = events.toVector
        )
      )
    )

  val nodeId1: NodeId = NodeId(0, 1)
  val nodeId2: NodeId = NodeId(0, 2)
  val nodeId3: NodeId = NodeId(0, 3)

  def spec: Spec[Any, Any] = suite("Edge Reconciliation")(
    test("Simple case: a spec with two nodes distinct nodes with a single edge between them") {

      val node1 = makeNode(id = nodeId1, Event.EdgeAdded(Edge("e1", nodeId2, EdgeDirection.Undirected)))
      val node2 = makeNode(id = nodeId2, Event.FarEdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected)))
      val node3 = Node.empty(nodeId3)

      val nodeSpecA = node("a")
      val nodeSpecB = node("b")
      val subgraphSpec = subgraph("subgraph name")(
        undirectedEdge(from = nodeSpecA, to = nodeSpecB)
      )

      // When matched starting at either node1 and node2, two matches are produced since the starting point will
      // match either node a or b in the graph.
      // When matching starting at node1 and node2, there will be a total of four matches, with two pairs of duplicates.
      // Matching starting at node3 never produces any matches since node3 has no edges.
      val expectedMatches = Set(
        Map("a" -> nodeId1, "b" -> nodeId2),
        Map("a" -> nodeId2, "b" -> nodeId1)
      )

      for
        matchesStartingAtAllThreeNodes <- subgraphSpec.matchTo(
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId1, nodeId2, nodeId3)
        )
        matchesStartingAtNode1 <- subgraphSpec.matchTo(
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId1)
        )
        matchesStartingAtNode2 <- subgraphSpec.matchTo(
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId2)
        )
      yield assertTrue(
        matchesStartingAtAllThreeNodes.length == 4,
        matchesStartingAtAllThreeNodes.toSet == expectedMatches,
        matchesStartingAtNode1.length == 2,
        matchesStartingAtNode1.toSet == expectedMatches,
        matchesStartingAtNode2.length == 2,
        matchesStartingAtNode2.toSet == expectedMatches
      )
    },
    test("Simple case: a spec with a single node (reflexive edge) and a spec with no qualification") {

      val node1 = makeNode(
        id = nodeId1,
        Event.EdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected)),
        Event.FarEdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected))
      )
      val node2 = Node.empty(nodeId2)

      val nodeSpecA = node("a")
      val subgraphSpec = subgraph("subgraph name")(
        undirectedEdge(from = nodeSpecA, to = nodeSpecA)
      )

      // There is only one possible match
      val expectedMatches = Set(
        Map("a" -> nodeId1)
      )

      for matches <- subgraphSpec.matchTo(
          graphNodes = Vector(node1, node2),
          matchStartingAt = Vector(nodeId1, nodeId2)
        )
      yield assertTrue(
        matches.length == 2,
        matches.toSet == expectedMatches
      )
    }
  )