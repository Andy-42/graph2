package andy42.graph.matcher

import zio.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed
import zio.test.*
import andy42.graph.matcher.node
import andy42.graph.matcher.NodePredicates.*
import andy42.graph.matcher.SomeOtherPredicateIdeas.*
import scala.util.hashing.MurmurHash3.stringHash

import andy42.graph.matcher.{NodeSpec, NodePredicate, SnapshotSelector}
object NodePredicateDSLSpec extends ZIOSpecDefault:

  def spec: Spec[Sized, Nothing] = suite("NodePredicateDSL")(
    test("A node predicate that changes the snapshot selector") {

      val spec: NodeSpec =
        node {
          usingNodeCurrent
          hasProperty("p")
          usingEventTime
          doesNotHaveProperty("p")
        }

      assertTrue(
        spec.spec == "node: snapshot @ node current => hasProperty(p), snapshot @ event time => doesNotHaveProperty(p)",
        spec.fingerprint == stringHash(spec.spec),
        spec.predicates.length == 2,
        spec.predicates(0).spec == "snapshot @ node current => hasProperty(p)",
        spec.predicates(0).fingerprint == stringHash(spec.predicates(0).spec),
        spec.predicates(0).isInstanceOf[NodePredicate.Snapshot],
        spec.predicates(0).asInstanceOf[NodePredicate.Snapshot].selector == SnapshotSelector.NodeCurrent,
        spec.predicates(1).spec == "snapshot @ event time => doesNotHaveProperty(p)",
        spec.predicates(1).fingerprint == stringHash(spec.predicates(1).spec),
        spec.predicates(1).isInstanceOf[NodePredicate.Snapshot],
        spec.predicates(1).asInstanceOf[NodePredicate.Snapshot].selector == SnapshotSelector.EventTime
      )
    },
    test("A node predicate that uses history filter") {
      import SomeOtherPredicateIdeas.*

      val spec: NodeSpec =
        node {
          hasPropertySetMoreThanNTimes("k", 42)
        }

      assertTrue(
        spec.spec == "node: history => hasPropertySetMoreThanNTimes(k,42)",
        spec.fingerprint == stringHash(spec.spec),
        spec.predicates.length == 1,
        spec.predicates(0).spec == "history => hasPropertySetMoreThanNTimes(k,42)",
        spec.predicates(0).fingerprint == stringHash(spec.predicates(0).spec),
        spec.predicates(0).isInstanceOf[NodePredicate.History],
        spec.predicates(0).fingerprint == stringHash(spec.predicates(0).spec)
      )
    }
  ) @@ timed
