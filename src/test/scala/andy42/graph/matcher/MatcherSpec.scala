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

  val appConfig: ULayer[AppConfig] = ZLayer.succeed(AppConfig())

  val matcherConfig1: MatcherConfig =
    MatcherConfig(
      allNodesInMutationGroupMustMatch = false
    )

  val matcherConfig2: MatcherConfig =
    MatcherConfig(
      allNodesInMutationGroupMustMatch = true
    )
  val matcherConfig3: MatcherConfig =
    MatcherConfig(
      allNodesInMutationGroupMustMatch = false
    )

  val trace: TaskLayer[Tracing & Tracer] = appConfig >>> TracingService.live

  /** Match the nodes in a mutationGroup to a SubgraphSpec.
    * @param subgraphSpec
    *   The SubgraphSpec that is being matched against.
    * @param graphNodes
    *   A collection of nodes that contains at least all reachable nodes in `matchStartingPoints`. In this test fixture,
    *   the Graph service is not used, so all nodes must be available from the cache. Since the Graph service is not
    *   used, it is up to the caller to construct a well-formed graph contents (e.g., half-edge consistency).
    * @param nodes
    *   Matching will use these NodeIds as starting points in matching. All these nodes must be in the cache, but there
    *   is no requirement that all nodes in the cache are starting points for matching.
    * @return
    *   All the matches for this subgraph spec starting matching at the given nodes.
    */
  def matchTo(
      subgraphSpec: SubgraphSpec,
      graphNodes: Vector[Node],
      nodes: Vector[Node],
      matcherConfig: MatcherConfig
  ): ZIO[AppConfig & Tracing & ContextStorage, PersistenceFailure | UnpackFailure, Seq[SpecNodeBindings]] =
    for
      tracing <- ZIO.service[Tracing]
      contextStorage <- ZIO.service[ContextStorage]
      matcher <- Matcher.make(
        config = matcherConfig, // Using specific MatcherConfig for testing different cases
        time = time,
        graph = UnimplementedGraph(),
        subgraphSpec = subgraphSpec,
        affectedNodes = graphNodes,
        tracing = tracing,
        contextStorage = contextStorage
      )
      subgraphMatches <- matcher.matchNodesToSubgraphSpec(nodes)
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

  def spec: Spec[Any, Any] = suite("Matcher")(
    test("Simple case: a spec with two nodes distinct nodes with a single edge between them") {

      val node1 = makeNode(
        id = nodeId1,
        Event.PropertyAdded("x", 1L),
        Event.EdgeAdded(Edge("e1", nodeId2, EdgeDirection.Undirected))
      )
      val node2 = makeNode(
        id = nodeId2,
        Event.PropertyAdded("x", 2L),
        Event.EdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected))
      )
      val node3 = Node.empty(nodeId3)
      val allNodes = Vector(node1, node2, node3)

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
        //        matchesWithUnmatchedNodeInMutationGroupWithConfig1 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId1, nodeId2, nodeId3),
//          matcherConfig = matcherConfig1
//        )
//        matchesWithUnmatchedNodeInMutationGroupWithConfig2 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId1, nodeId2, nodeId3),
//          matcherConfig = matcherConfig2
//        )
//        matchesWithUnmatchedNodeInMutationGroupWithConfig3 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId1, nodeId2, nodeId3),
//          matcherConfig = matcherConfig3
//        )

        matchesWithFullyMatchedMutationGroupWithConfig1 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1, node2),
          matcherConfig = matcherConfig1
        )
//        matchesWithFullyMatchedMutationGroupWithConfig2 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId1, nodeId2),
//          matcherConfig = matcherConfig2
//        )
//        matchesWithFullyMatchedMutationGroupWithConfig3 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId1, nodeId2),
//          matcherConfig = matcherConfig3
//        )
//
//        matchesWithPartiallyMatchedMutationGroupWithConfig1 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId2),
//          matcherConfig = matcherConfig1
//        )
//        matchesWithPartiallyMatchedMutationGroupWithConfig2 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId2),
//          matcherConfig = matcherConfig2
//        )
//        matchesWithPartiallyMatchedMutationGroupWithConfig3 <- matchTo(
//          subgraphSpec = subgraphSpec,
//          graphNodes = allNodes,
//          mutationGroup = Vector(nodeId2),
//          matcherConfig = matcherConfig3
//        )
      yield assertTrue(
//        // This will match because the unmatched node3 in the mutation group doesn't need to be matched by this config
//        matchesWithUnmatchedNodeInMutationGroupWithConfig1.toSet == expectedMatches,
//        // The configurations with allNodesInMutationGroupMustMatch... will not not match because node3 never matches
//        matchesWithUnmatchedNodeInMutationGroupWithConfig2.toSet == Set.empty,
//        matchesWithUnmatchedNodeInMutationGroupWithConfig3.toSet == Set.empty,

        // When all nodes in the mutation group are matched, all matcher configurations will match
        matchesWithFullyMatchedMutationGroupWithConfig1.toSet == expectedMatches
//        matchesWithFullyMatchedMutationGroupWithConfig2.toSet == expectedMatches, // fail - empty set
//        matchesWithFullyMatchedMutationGroupWithConfig3.toSet == expectedMatches, // fail - empty set

        // When the mutation group is doesn't include one of the nodes in the match,
        // the allNodesInMutationGroupMustMatch... configurations will not match.
//        matchesWithPartiallyMatchedMutationGroupWithConfig1.toSet == expectedMatches,
//        matchesWithPartiallyMatchedMutationGroupWithConfig2.toSet == expectedMatches, // ???
//        matchesWithPartiallyMatchedMutationGroupWithConfig3.toSet == Set.empty

        // TODO: Need a test to specifically distinguish the behavioural difference between allNodesInMutationGroupMustMatch/AndBeConnected
      )
    },
    test("Simple case: a spec with a single node (reflexive edge) and a spec with no qualification") {

      val node1 = makeNode(
        id = nodeId1,
        Event.EdgeAdded(Edge("e1", nodeId1, EdgeDirection.Undirected)),
        Event.PropertyAdded("x", 1L)
      )
      val node2 = Node.empty(nodeId2)
      val allNodes = Vector(node1, node2)

      val nodeSpecA = node("a").hasProperty("x") // predicate is so anchor the node as match starting point
      val subgraphSpec = subgraph("subgraph name")(
        undirectedEdge(from = nodeSpecA, to = nodeSpecA)
      )

      // There is only one possible match
      val expectedMatches = Set(
        Map("a" -> nodeId1)
      )

      for
        matchesWithUnmatchedNodeInMutationGroupWithConfig1 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1, node2),
          matcherConfig = matcherConfig1
        )
        matchesWithUnmatchedNodeInMutationGroupWithConfig2 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1, node2),
          matcherConfig = matcherConfig2
        )
        matchesWithUnmatchedNodeInMutationGroupWithConfig3 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1, node2),
          matcherConfig = matcherConfig3
        )

        matchesWithFullyMatchedMutationGroupWithConfig1 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1),
          matcherConfig = matcherConfig1
        )
        matchesWithFullyMatchedMutationGroupWithConfig2 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1),
          matcherConfig = matcherConfig2
        )
        matchesWithFullyMatchedMutationGroupWithConfig3 <- matchTo(
          subgraphSpec = subgraphSpec,
          graphNodes = allNodes,
          nodes = Vector(node1),
          matcherConfig = matcherConfig3
        )
      yield assertTrue(
        // With config1, an unmatched node in the mutation group doesn't prevent the match
        matchesWithUnmatchedNodeInMutationGroupWithConfig1.toSet == expectedMatches,
        // With config2 and config3, all nodes in the mutation group must match
        matchesWithUnmatchedNodeInMutationGroupWithConfig2.toSet == Set.empty,
        matchesWithUnmatchedNodeInMutationGroupWithConfig3.toSet == Set.empty,

        // When there are no unmatched nodes in the mutation group, all configs result in a match
        matchesWithFullyMatchedMutationGroupWithConfig1.toSet == expectedMatches,
        matchesWithFullyMatchedMutationGroupWithConfig2.toSet == expectedMatches,
        matchesWithFullyMatchedMutationGroupWithConfig3.toSet == expectedMatches
      )
    }
  ).provide(trace, appConfig, ContextStorage.fiberRef) @@ TestAspect.ignore
