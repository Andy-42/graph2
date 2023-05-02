package andy42.graph.matcher

import andy42.graph.matcher.EdgeSpecs.*
import andy42.graph.model.*
import zio.test.*

import scala.collection.immutable.List

object SubgraphSpecSpec extends ZIOSpecDefault:

  override def spec: Spec[Any, Any] =
    suite("SubgraphSpec")(
      test("Basic node and edge spec enumeration") {

        val node1 = node("n1")
        val node2 = node("n2")
        val node3 = node("n3")

        val edgeSpec1to2 = directedEdge(from = node1, to = node2)
        val edgeSpec1to3 = directedEdge(from = node1, to = node3)
        // The reversed edge specs will be created automatically to represent the other half-edge
        val edgeSpec2to1 = edgeSpec1to2.reverse
        val edgeSpec3to1 = edgeSpec1to3.reverse

        val subgraphSpec = subgraph("s")(edgeSpec1to2, edgeSpec1to3)

        val expectedIncoming = Map(
          "n1" -> List(edgeSpec2to1, edgeSpec3to1),
          "n2" -> List(edgeSpec1to2),
          "n3" -> List(edgeSpec1to3)
        )

        val expectedOutgoing = Map(
          "n1" -> List(edgeSpec1to2, edgeSpec1to3),
          "n2" -> List(edgeSpec2to1),
          "n3" -> List(edgeSpec3to1)
        )

        // SubgraphSpec sorts edges and node names so that they appear in lists in a predictable order

        assertTrue(
          // Even though there are only two edge specs, they are expanded to 4 half-edges
          subgraphSpec.allHalfEdges.length == 4,
          subgraphSpec.allNodeSpecs == List(node1, node2, node3),
          subgraphSpec.allNodeSpecNames == List("n1", "n2", "n3"),
          // The incoming and outgoing edges for each node.
          subgraphSpec.incomingEdges == expectedIncoming,
          subgraphSpec.outgoingEdges == expectedOutgoing,
          // Even if we specify the edges from the other end, it generates the same half-edges
          subgraph("s")(edgeSpec2to1, edgeSpec3to1).incomingEdges == expectedIncoming,
          subgraph("s")(edgeSpec1to2, edgeSpec3to1).incomingEdges == expectedIncoming,
          subgraph("s")(edgeSpec2to1, edgeSpec3to1).outgoingEdges == expectedOutgoing,
          subgraph("s")(edgeSpec1to2, edgeSpec3to1).outgoingEdges == expectedOutgoing,
          subgraphSpec.duplicateNodeSpecNames.isEmpty
        )
      },
      test("connected nodes and connected subgraphs") {

        val node1 = node("n1")
        val node2 = node("n2")
        val node3 = node("n3")
        val node4 = node("n4")
        val node5 = node("n5")
        val node6 = node("n6")

        val subgraphSpec = subgraph("s")(
          // A subgraph that loops back on itself
          directedEdge(from = node1, to = node2),
          directedEdge(from = node2, to = node3),
          directedEdge(from = node3, to = node1),
          // A separate unconnected subgraph - no loop, but multiple
          // edges connect node4 and node6 in the connected subgraph
          directedEdge(from = node4, to = node5),
          directedEdge(from = node4, to = node6),
          directedEdge(from = node5, to = node6)
        )

        assertTrue(
          subgraphSpec.allConnectedNodes("n1") == Set("n1", "n2", "n3"),
          subgraphSpec.allConnectedNodes("n2") == Set("n1", "n2", "n3"),
          subgraphSpec.allConnectedNodes("n3") == Set("n1", "n2", "n3"),
          subgraphSpec.allConnectedNodes("n4") == Set("n4", "n5", "n6"),
          subgraphSpec.allConnectedNodes("n5") == Set("n4", "n5", "n6"),
          subgraphSpec.allConnectedNodes("n6") == Set("n4", "n5", "n6"),
          subgraphSpec.connectedSubgraphs() == Set(Set("n1", "n2", "n3"), Set("n4", "n5", "n6")),
          subgraphSpec.duplicateNodeSpecNames.isEmpty
        )
      },
      test("Detect duplicate NodeSpec names") {

        val node1 = node("n1")
        val node2 = node("n1")
        val node3 = node("n3")

        val subgraphSpec = subgraph("s")(
          directedEdge(from = node1, to = node2),
          directedEdge(from = node1, to = node3)
        )

        assertTrue(
          subgraphSpec.duplicateNodeSpecNames == List("n1")
        )
      }
    )
