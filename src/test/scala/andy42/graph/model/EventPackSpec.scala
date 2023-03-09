package andy42.graph.model

import andy42.graph.model.EventPackSpec.test
import zio.*
import zio.test.Assertion.*
import zio.test.TestAspect.timed
import zio.test.*

object EventPackSpec extends ZIOSpecDefault:

  def spec = suite("Domain model packing/unpacking")(
    test("Events can be round-tripped through packed form")(
      check(Generators.genEvent)((originalEvent: Event) =>
        val packed = originalEvent.toPacked

        for roundTrippedEvent <- Event.unpacked(packed)
        yield assertTrue(originalEvent == roundTrippedEvent)
      )
    ),
    test("EventsAtTime can be round-tripped through packed form")(
      check(Generators.genEventsAtTime)((originalEventsAtTime: EventsAtTime) =>
        val packed = originalEventsAtTime.toPacked

        for roundTrippedEventsAtTime <- EventsAtTime.unpacked(packed)
        yield assertTrue(originalEventsAtTime == roundTrippedEventsAtTime)
      )
    ),
    test("NodeHistory is packed as an uncounted sequence")(
      check(Gen.vectorOf1(Generators.genEventsAtTime))((nodeHistory: NodeHistory) =>
        val packedAllTogether = nodeHistory.toPacked
        val packedSeparately = nodeHistory.toArray.flatMap(_.toPacked)

        assertTrue(packedAllTogether.sameElements(packedSeparately))
      )
    ),
    test("NodeHistory can be round-tripped through packed form")(
      check(Gen.vectorOf1(Generators.genEventsAtTime))((originalNodeHistory: NodeHistory) =>
        val packed = originalNodeHistory.toPacked

        for roundTrippedNodeHistory <- NodeHistory.unpacked(packed)
        yield assertTrue(originalNodeHistory == roundTrippedNodeHistory)
      )
    )
  ) @@ timed
