package andy42.graph.model

import zio.*
import zio.test.*
import zio.test.Assertion.*

object EdgeReconciliationSpec extends ZIOSpecDefault:

  val nodeIdGen: Gen[Sized, NodeId] = 
    for id <- Gen.vectorOfN(16)(Gen.byte)
    yield NodeId(id.toArray)

  val edgeDirectionGen: Gen[Sized, EdgeDirection] =
    Gen.oneOf(Gen.const(EdgeDirection.Incoming), Gen.const(EdgeDirection.Outgoing), Gen.const(EdgeDirection.Undirected))

  val edgeGen: Gen[Sized, Edge] =
    for
      k <- Gen.string
      other <- nodeIdGen
      direction <- edgeDirectionGen
      isNear <- Gen.boolean
    yield if isNear then NearEdge(k, other, direction) else FarEdge(k, other, direction)

  val idAndEdgeGen: Gen[Sized, (NodeId, Edge)] =
    for
      id <- nodeIdGen
      edge <- edgeGen
    yield (id, edge)

  def spec: Spec[Sized, Nothing] = suite("Edge Reconciliation")(
    test("Edge.reverse round-trips") {
      check(nodeIdGen, edgeGen) { (id, edge) =>
        assertTrue(edge.reverse(id).reverse(edge.other) == edge)
      }
    },
    test("The hash of an edge and its reverse reconcile") {
      check(nodeIdGen, edgeGen) { (id, edge) =>
        val hash = edge.hash(id)
        val reversedHash = edge.reverse(id).hash(edge.other)
        assertTrue((hash ^ reversedHash) == 0L)
      }
    },
    test("Hashes can be combined in any order") {
      check(Gen.listOf(idAndEdgeGen)) { idsAndEdges =>
        val nearHashes = idsAndEdges.map((id, edge) => edge.hash(id))
        val farHashes = idsAndEdges.map((id, edge) => edge.reverse(id).hash(edge.other))

        val allHashes = scala.util.Random.shuffle(nearHashes ++ farHashes)
        val reconciliation = allHashes.iterator.fold(0L)(_ ^ _)

        assertTrue(reconciliation == 0L)
      }
    }
  )
