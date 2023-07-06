package andy42.graph.matcher

import andy42.graph.matcher.MatcherSpec.{suite, test}
import zio.Scope
import zio.test.*

object CombinationGeneratorSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment with Scope, Any] = suite("CombinationGenerator")(
    test("All combinations") {

      val x = Vector(
        Vector(1),
        Vector(1, 2, 3)
      )

      val expected = Vector(
        Vector(1, 1),
        Vector(1, 2),
        Vector(1, 3)
      )

      assertTrue(CombinationGenerator.all(x) == expected)
    },
    test("All combinations where an element is only used once") {

      val x = Vector(
        Vector(1),
        Vector(1, 2, 3)
      )

      val expected = Vector(
        // Vector(1, 1), // Not this one since 1 can only appear once in the result
        Vector(1, 2),
        Vector(1, 3)
      )

      assertTrue(CombinationGenerator.combinationsSingleUse(x) == expected)
    },
    test("Any input with zero elements at the second level will generate zero elements") {

      val x = Vector(
        Vector(1),
        Vector(),
        Vector(1, 2, 3)
      )

      val expected = Vector.empty

      assertTrue(CombinationGenerator.combinationsSingleUse(x) == expected)
    }
  )
