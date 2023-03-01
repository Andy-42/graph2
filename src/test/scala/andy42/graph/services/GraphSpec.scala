package andy42.graph.services

import zio._
import zio.test.{test, _}
import zio.test.Assertion._
import java.util.UUID

import scala.util.Random

import andy42.graph.model._

object GraphSpec extends ZIOSpecDefault:

  def genNodeId: NodeId =
    val a = Array.ofDim[Byte](16)
    Random.nextBytes(a)
    a.toVector

  override def spec: Spec[Any, Any] =
    suite("Graph")(
      test("Test1") {

        val id = genNodeId
        val time: EventTime = Random.nextLong()
        
        val inputMutation = Event.PropertyAdded("p", 42)

        val expectedCurrent = NodeSnapshot(time = time, sequence = 0, properties = Map("p" -> 42), edges = Set.empty)

        val expectedPropertyAddedEvents = Vector(inputMutation)
        val expectedEventsAtTime = EventsAtTime(time = time, sequence = 0, events = expectedPropertyAddedEvents)

        val expectedHistory = Vector(expectedEventsAtTime)
        val expectedNode = Node.fromHistory(id, expectedHistory)

        val expectedOutputEvents = Vector(
          time -> Vector(GroupedGraphMutationOutput(expectedNode, expectedPropertyAddedEvents))
        )

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

          nodeFromData <- rememberingNodeData.get(id)
          nodeFromCacheAfter <- rememberingNodeCache.get(id)
          standingQueryEvaluationParameters <- testStandingQueryEvaluation.graphChangedParameters
          edgeSynchronizationParameters <- testEdgeSynchronization.graphChangedParameters
          _ = true
        yield assert(nodeBefore.hasEmptyHistory)(isTrue) &&
          assert(nodeBefore.id)(equalTo(id)) &&
          assert(nodeFromGraphAfter)(equalTo(expectedNode)) &&
          assert(nodeCurrentAfter)(equalTo(expectedCurrent)) &&
          assert(nodeFromData)(equalTo(expectedNode)) &&
          assert(inFlightBefore.isEmpty)(isTrue) &&
          assert(inFlightAfter.isEmpty)(isTrue) &&
          assert(nodeFromCacheAfter)(equalTo(Some(expectedNode))) &&
          assert(standingQueryEvaluationParameters.toVector)(equalTo(expectedOutputEvents)) &&
          assert(edgeSynchronizationParameters.toVector)(equalTo(expectedOutputEvents))
      }.provide(
        Graph.layer,
        RememberingNodeData.layer,
        RememberingNodeCache.layer,
        TestStandingQueryEvaluation.layer,
        TestEdgeSynchronization.layer
      )
    )
