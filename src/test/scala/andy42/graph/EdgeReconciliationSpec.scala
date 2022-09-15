package andy42.graph.model

import zio._
import zio.test._
import zio.test.Assertion._

object EdgeReconciliationSpec extends ZIOSpecDefault:

  val nodeIdGen: Gen[Sized, NodeId] = Gen.vectorOfN(16)(Gen.byte)

  val edgeGen: Gen[Sized, Edge] =
    for
      k <- Gen.string
      other <- nodeIdGen
    yield Edge(k, other)

  val idAndEdgeGen: Gen[Sized, (NodeId, Edge)] =
    for
      id <- nodeIdGen
      edge <- edgeGen
    yield (id, edge)

  def spec = suite("Edge Reconciliation")(
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
        val reconciliation = allHashes.fold(0L)(_ ^ _)
        
        assertTrue(reconciliation == 0L)
      }
    }
  )
