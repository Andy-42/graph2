package andy42.graph.services

import zio.*
import zio.test.{test, *}
import zio.test.Assertion.*

import java.util.UUID
import scala.util.Random
import andy42.graph.model.*

object GraphSpec extends ZIOSpecDefault:

  def genNodeId: NodeId =
    val a = Array.ofDim[Byte](16)
    Random.nextBytes(a)
    a.toVector

  def testGraphDataFlow(
      id: NodeId,
      time: EventTime,
      inputMutation: Event,
      expectedNode: Node,
      expectedCurrent: NodeSnapshot,
      expectedOutputEvents: Vector[(EventTime, Vector[GroupedGraphMutationOutput])]
  ): ZIO[Graph, Any, TestResult] =
    for
      graph <- ZIO.service[Graph]

      // Get mock services used for testing from GraphLive
      graphLive = graph.asInstanceOf[GraphLive]
      rememberingNodeData = graphLive.nodeDataService.asInstanceOf[RememberingNodeDataService]
      rememberingNodeCache = graphLive.cache.asInstanceOf[RememberingNodeCacheService]
      testStandingQueryEvaluation = graphLive.standingQueryEvaluation.asInstanceOf[TestStandingQueryEvaluation]
      testEdgeSynchronization = graphLive.edgeSynchronization.asInstanceOf[TestEdgeSynchronization]

      // Observe graph state before the change
      inFlightBefore <- graphLive.inFlight.toList.commit
      nodeBefore <- graph.get(id)

      // Mutate the graph state
      mutations = Vector(GraphMutationInput(id, inputMutation))
      _ <- graph.append(time, mutations)

      // Observe the graph state after the change
      inFlightAfter <- graphLive.inFlight.toList.commit
      nodeFromGraphAfter <- graph.get(id)
      nodeCurrentAfter <- nodeFromGraphAfter.current

      // Services that are updated with the new node state
      nodeFromData <- rememberingNodeData.get(id)
      nodeFromCacheAfter <- rememberingNodeCache.get(id)
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
    (RememberingNodeData.layer ++
      RememberingNodeCache.layer ++
      TestStandingQueryEvaluation.layer ++
      TestEdgeSynchronization.layer) >>> Graph.layer

  override def spec =
    suite("Graph")(
      test("Test1") {

        // TODO: Make this into property based test

        val id = genNodeId
        val time: EventTime = Random.nextLong()

        val inputMutation = Event.PropertyAdded("p", 42)

        val expectedPropertyAddedEvents = Vector(inputMutation)
        val expectedEventsAtTime = EventsAtTime(time = time, sequence = 0, events = expectedPropertyAddedEvents)

        val expectedCurrent = NodeSnapshot(time = time, sequence = 0, properties = Map("p" -> 42), edges = Set.empty)
        val expectedHistory = Vector(expectedEventsAtTime)
        val expectedNode = Node.fromHistory(id, expectedHistory)

        val expectedOutputEvents = Vector(
          time -> Vector(GroupedGraphMutationOutput(expectedNode, expectedPropertyAddedEvents))
        )

        testGraphDataFlow(id, time, inputMutation, expectedNode, expectedCurrent, expectedOutputEvents)
      }.provide(graphLayer)
    )
