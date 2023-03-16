package andy42.graph.matcher

import zio.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed
import zio.test.*
import andy42.graph.matcher.node
import andy42.graph.matcher.NodePredicates.*
import andy42.graph.matcher.SomeOtherPredicateIdeas.*

object NodePredicateDSLSpec extends ZIOSpecDefault:

  def spec: Spec[Sized, Nothing] = suite("NodePredicateDSL")(
    test("A node predicate that changes the snapshot selector") {

      val description = "Property is currently set, but is not set at the event time"
      val spec =
        node(description) {
          usingNodeCurrent
          hasProperty("p")
          usingEventTime
          doesNotHaveProperty("p")
        }

      assertTrue(
        spec.description == description,
        spec.predicates.length == 2,
        spec.predicates(0) match {
          case NodePredicate.SnapshotPredicate(SnapshotSelector.NodeCurrent, _) => true
          case _                                                                => false
        },
        spec.predicates(1) match {
          case NodePredicate.SnapshotPredicate(SnapshotSelector.EventTime, _) => true
          case _                                                              => false
        }
      )
    },
    test("A node predicate that uses history filter") {
      val description = "Has property set more than n times"
      val spec = node(description) {
        historyFilter(hasPropertySetMoreThanNTimes("k", 42))
      }

      assertTrue(
        spec.description == description,
        spec.predicates.length == 1,
        spec.predicates(0) match {
          case NodePredicate.HistoryPredicate(_) => true
          case _                                 => false
        }
      )
    }
  )
