package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.test.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed

import java.util.UUID

object GraphSpec extends ZIOSpecDefault:

  def testGraphDataFlow(
      id: NodeId,
      time: EventTime,
      inputMutations: Vector[NodeMutationInput],
      expectedNode: Node,
      expectedCurrent: NodeSnapshot,
      expectedOutputEvents: Vector[(EventTime, Vector[NodeMutationOutput])]
  ): ZIO[Graph, Any, TestResult] =
    for
      graph <- ZIO.service[Graph]

      // Get mock services used for testing from GraphLive
      graphLive = graph.asInstanceOf[GraphLive]
      testNodeRepository = graphLive.nodeDataService.asInstanceOf[TestNodeRepositoryLive]
      testNodeCache = graphLive.cache.asInstanceOf[TestNodeCacheLive]
      testStandingQueryEvaluation = graphLive.standingQueryEvaluation.asInstanceOf[TestStandingQueryEvaluation]
      testEdgeSynchronization = graphLive.edgeSynchronization.asInstanceOf[TestEdgeSynchronization]
      _ <- testNodeCache.clear() *> testNodeRepository.clear()

      // Observe graph state before the change
      inFlightBefore <- graphLive.inFlight.toList.commit
      nodeBefore <- graph.get(id)

      // Mutate the graph state
      _ <- graph.append(time, inputMutations)

      // Observe the graph state after the change
      inFlightAfter <- graphLive.inFlight.toList.commit
      nodeFromGraphAfter <- graph.get(id)
      nodeCurrentAfter <- nodeFromGraphAfter.current

      // Services that are updated with the new node state
      nodeFromData <- testNodeRepository.get(id)
      nodeFromCacheAfter <- testNodeCache.get(id)
      // Services that get notified of changes to the node state
      standingQueryEvaluationParameters <- testStandingQueryEvaluation.graphChangedParameters
      edgeSynchronizationParameters <- testEdgeSynchronization.graphChangedParameters
      _ = true
    yield
    // There are no nodes in progress by the graph either before or after
    assert(inFlightBefore.isEmpty)(isTrue) &&
      assert(inFlightAfter.isEmpty)(isTrue) &&
      // The node (as obtained from the graph) starts out empty and is modified as expected
      assert(nodeBefore.hasEmptyHistory)(isTrue) &&
      assert(nodeFromGraphAfter)(equalTo(expectedNode)) &&
      assert(nodeCurrentAfter)(equalTo(expectedCurrent)) && // redundant test
      // The data service and the cache have been updated with the new node state
      assert(nodeFromData)(equalTo(expectedNode)) &&
      assert(nodeFromCacheAfter)(isSome(equalTo(expectedNode))) &&
      // The standing query evaluation and edge synchronization services have been notified of the changes
      assert(standingQueryEvaluationParameters.toVector)(equalTo(expectedOutputEvents)) &&
      assert(edgeSynchronizationParameters.toVector)(equalTo(expectedOutputEvents))

  val graphLayer: ULayer[Graph] =
    (TestNodeRepository.layer ++
      TestNodeCache.layer ++
      TestStandingQueryEvaluation.layer ++
      TestEdgeSynchronization.layer) >>> Graph.layer

  val genNodeId: Gen[Any, NodeId] =
    for id <- Gen.vectorOfN(NodeId.byteLength)(Gen.byte)
    yield NodeId(id)

  override def spec: Spec[Any, Any] =
    suite("Graph")(
      test("Simplest possible test that touches all data flows") {
        check(genNodeId, Gen.long, Gen.long) { (id, time, p1Value) =>

          val edge = NearEdge("e1", id, EdgeDirection.Outgoing) // Reflexive - points back to originating node
          val inputEvents = Vector(Event.PropertyAdded("p1", p1Value), Event.EdgeAdded(edge))
          val inputMutations = Vector(NodeMutationInput(id, inputEvents))

          val expectedEventsAtTime = EventsAtTime(time = time, sequence = 0, events = inputEvents)
          val expectedCurrent =
            NodeSnapshot(time = time, sequence = 0, properties = Map("p1" -> p1Value), edges = Set(edge))
          val expectedHistory = Vector(expectedEventsAtTime)
          val expectedNode = Node.fromHistory(id, expectedHistory)
          val expectedMutationOutput = Vector(time -> Vector(NodeMutationOutput(expectedNode, inputEvents)))

          testGraphDataFlow(id, time, inputMutations, expectedNode, expectedCurrent, expectedMutationOutput)
        }
      }.provide(graphLayer)
    ) @@ timed
