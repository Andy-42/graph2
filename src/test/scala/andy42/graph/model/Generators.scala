package andy42.graph.model

import andy42.graph.model.NodeId
import zio.*
import zio.test.Gen

import java.time.Instant

object Generators:

  val genNodeId: Gen[Any, NodeId] =
    Gen.fromRandom(random =>
      for byteChunk <- random.nextBytes(NodeId.byteLength)
      yield NodeId(byteChunk.toVector)
    )

  val genPropertyName: Gen[Any, String] =
    Gen.string

  val genEdgeDirection: Gen[Any, EdgeDirection] =
    Gen.oneOf(Gen.const(EdgeDirection.Incoming), Gen.const(EdgeDirection.Outgoing), Gen.const(EdgeDirection.Undirected))

  val genEdge: Gen[Any, EdgeImpl] =
    for
      other <- genNodeId
      propertyName <- genPropertyName
      edgeDirection <- genEdgeDirection
    yield EdgeImpl(propertyName, other, edgeDirection)

  val genEdgeEvent: Gen[Any, Event.EdgeAdded | Event.EdgeRemoved ] =
    for
      edge <- genEdge
      isAdded <- Gen.boolean
    yield (edge, isAdded) match
      case (edge: EdgeImpl, true)  => Event.EdgeAdded(edge)
      case (edge: EdgeImpl, false) => Event.EdgeRemoved(edge)

  val genNilProperty: Gen[Any, Unit] = Gen.const(())
  val genBooleanProperty: Gen[Any, Boolean] = Gen.boolean
  val genIntegerProperty: Gen[Any, Long] = Gen.long
  val genFloatProperty: Gen[Any, Double] = Gen.double
  val genStringProperty: Gen[Any, String] = Gen.string
  val genBinaryProperty: Gen[Any, BinaryValue] =
    for v <- Gen.vectorOf(Gen.byte)
    yield BinaryValue(v)
  val genTimestampProperty: Gen[Any, Instant] = Gen.instant

  val genScalarProperty: Gen[Any, ScalarType] = Gen.oneOf(
    genNilProperty,
    genBooleanProperty,
    genIntegerProperty,
    genFloatProperty,
    genStringProperty,
    genBinaryProperty,
    genTimestampProperty
  )

  val genArrayProperty: Gen[Any, PropertyArrayValue] =
    for a <- Gen.vectorOf(genScalarProperty)
    yield PropertyArrayValue(a)

  val genMapProperty: Gen[Any, PropertyMapValue] =
    for m <- Gen.mapOf(Gen.string, genScalarProperty)
    yield PropertyMapValue(m)

  val genPropertyValue: Gen[Any, PropertyValueType] =
    Gen.weighted((genScalarProperty, 0.9), (genArrayProperty, 0.05), (genMapProperty, 0.05))

  val genPropertyAddedEvent: Gen[Any, Event.PropertyAdded] =
    for
      k <- Gen.string
      value <- genPropertyValue
    yield Event.PropertyAdded(k, value)

  val genPropertyRemovedEvent: Gen[Any, Event.PropertyRemoved] =
    for k <- Gen.string
    yield Event.PropertyRemoved(k)

  val genPropertyEvent: Gen[Any, Event.PropertyAdded | Event.PropertyRemoved] =
    Gen.weighted(
      (genPropertyAddedEvent, 0.9),
      (genPropertyRemovedEvent, 0.1)
    )

  val genNodeRemovedEvent: Gen[Any, Event] =
    Gen.const(Event.NodeRemoved)

  val genEvent: Gen[Any, Event] =
    Gen.weighted(
      (genEdgeEvent, 0.45),
      (genPropertyEvent, 0.45),
      (genNodeRemovedEvent, 0.1)
    )

  val genEventTime: Gen[Any, EventTime] = Gen.long
  
  val genSequence: Gen[Any, Int] = Gen.int(min = 0, max = Int.MaxValue)
  
  val genEvents: Gen[Any, Vector[Event]] = Gen.vectorOf1(genEvent)
  
  val genEventsAtTime: Gen[Any, EventsAtTime] =
    for
      time <- genEventTime
      sequence <- genSequence
      events <- genEvents
    yield EventsAtTime(time, sequence, events)
