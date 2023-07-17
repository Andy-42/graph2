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

      assertTrue(CombinationGenerator.generateCombinations(x) == expected)
    },
    test("Any input with zero elements at the second level will generate zero elements") {

      val x1 = Vector(
        Vector(),
        Vector(1, 2),
        Vector(1, 2, 3)
      )

      val x2 = Vector(
        Vector(1),
        Vector(),
        Vector(1, 2, 3)
      )

      val x3 = Vector(
        Vector(1),
        Vector(1, 2),
        Vector()
      )

      val expected = Vector.empty

      assertTrue(
        CombinationGenerator.generateCombinations(x1) == expected,
        CombinationGenerator.generateCombinations(x2) == expected,
        CombinationGenerator.generateCombinations(x3) == expected
      )
    },
    test("Using emptyCombinations as an input produces an empty") {

      // While any combination with an empty will produce an empty, it might
      // involve the combination generator doing more work than it needs to.
      // When the
      val empty = CombinationGenerator.emptyInput[Int]

      assertTrue(CombinationGenerator.generateCombinations(empty) == Vector.empty)
    }
  )
