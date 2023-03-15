package andy42.graph.matcher

import zio.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed
import zio.test.*

object NodePredicateDSLSpec extends ZIOSpecDefault:

  import NodePredicates.*

  def propertyCurrentlySetButNotAtEventTime(k: String): NodeSpec =
    node("Property is currently set, but is not set at the event time") {
      usingNodeCurrent
      hasProperty(k)
      usingEventTime
      doesNotHaveProperty(k)
    }

  import SomeOtherPredicateIdeas.*

  def propertySetMoreThanNTimes(k: String, n: Int) =
    node("Has property set more than n times") {
      historyFilter(hasPropertySetMoreThanNTimes(k, n))
    }


  def spec: Spec[Sized, Nothing] = suite("NodePredicateDSL")(
    test(""){

        val spec = propertyCurrentlySetButNotAtEventTime("myProperty")

        assertTrue(
            spec.description == "Property is currently set, but is not set at the event time",
            spec.predicates.length == 2
        )
    }
  )  
