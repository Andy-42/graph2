package andy42.graph.matcher

import andy42.graph.config.{AppConfig, TracerConfig, MatcherConfig}
import andy42.graph.matcher.EdgeSpecs.{directedEdge, undirectedEdge}
import andy42.graph.model.*
import andy42.graph.model.Generators.*
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.TracingService
import io.opentelemetry.api.trace.Tracer
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

object MatcherSpec extends ZIOSpecDefault:

  /** All tests in this fixture use a test at a single event time, the value of which doesn't matter in these tests. */
  val time: EventTime = 42

  val appConfigLayer: ULayer[AppConfig] = ZLayer.succeed(AppConfig())
  val trace: TaskLayer[Tracing & Tracer] = appConfigLayer >>> TracingService.live

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
        tracer: Tracing,
        graphNodes: Vector[Node],
        matchStartingAt: Vector[NodeId]
    ): ZIO[AppConfig, PersistenceFailure | UnpackFailure, Vector[ResolvedMatches]] =
      for
        config <- ZIO.service[AppConfig]
        matcher <- Matcher.make(
          config = config.matcherConfig,
          time = time,
          graph = UnimplementedGraph(), // unused - all nodes will be fetched from the cache
          tracing = tracer,
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

      val node1 = makeNode(
        id = nodeId1,
        Event.PropertyAdded("x", 1L),
        Event.EdgeAdded(Edge("e1", nodeId2, EdgeDirection.Undirected))
      )
      val node2 = makeNode(
        id = nodeId2,
        Event.PropertyAdded("x", 2L),
        Event.FarEdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected))
      )
      val node3 = Node.empty(nodeId3)

      // The matcher algorithm won't consider a node as a match starting point if it is unqualified,
      // so add some sort of predicate.
      val nodeSpecA = node("a").hasProperty("x")
      val nodeSpecB = node("b").hasProperty("x")
      val subgraphSpec = subgraph("subgraph name")(
        undirectedEdge(from = nodeSpecA, to = nodeSpecB)
      )

      // When matched starting at either node1 and node2, two matches are produced since the starting point will
      // match either node a or b in the graph.
      // When matching starting at node1 and node2, there will be a total of four matches, with two pairs of duplicates.
      // However, two of these pairs are duplicates.
      // Matching starting at node3 never produces any matches since node3 has no edges.
      val expectedMatches = Set(
        Map(
          "a" -> nodeId1,
          "b" -> nodeId2
        ),
        Map(
          "b" -> nodeId1,
          "a" -> nodeId2
        )
      )

      for
        tracing <- ZIO.service[Tracing]

        matchesStartingAtAllThreeNodes <- subgraphSpec.matchTo(
          tracing,
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId1, nodeId2, nodeId3)
        )
        matchesStartingAtNode1 <- subgraphSpec.matchTo(
          tracing,
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId1)
        )
        matchesStartingAtNode2 <- subgraphSpec.matchTo(
          tracing,
          graphNodes = Vector(node1, node2, node3),
          matchStartingAt = Vector(nodeId2)
        )
      yield assertTrue(
        matchesStartingAtAllThreeNodes.length == 2,
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
        Event.FarEdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected)),
        Event.PropertyAdded("x", 1L)
      )
      val node2 = Node.empty(nodeId2)

      val nodeSpecA = node("a").hasProperty("x") // predicate is so anchor the node as match starting point
      val subgraphSpec = subgraph("subgraph name")(
        undirectedEdge(from = nodeSpecA, to = nodeSpecA)
      )

      // There is only one possible match
      val expectedMatches = Set(
        Map("a" -> nodeId1)
      )

      for
        tracing <- ZIO.service[Tracing]
        matches <- subgraphSpec.matchTo(
          tracing,
          graphNodes = Vector(node1, node2),
          matchStartingAt = Vector(nodeId1, nodeId2)
        )
      yield assertTrue(
        matches.length == 1,
        matches.toSet == expectedMatches
      )
    }
  ).provide(trace, appConfigLayer)
