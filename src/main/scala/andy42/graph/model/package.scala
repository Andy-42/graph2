package andy42.graph

import java.time.Instant
import zio.IO

package object model {

  type ScalarType = Unit | 
    Boolean | 
    Int | Long | // Integral types will be promoted to Long
    Float | Double | // Floating point types will be promoted to Double
    String | 
    BinaryValue | 
    Instant

  case class BinaryValue(value: Vector[Byte])
  case class PropertyArrayValue(value: Vector[ScalarType])
  case class PropertyMapValue(value: Map[String, ScalarType])  

  type PropertyValueType = ScalarType | PropertyArrayValue | PropertyMapValue

  type NodeId = Vector[Byte] // Always 8 bytes

  type EventTime = Long // Epoch Millis
  val StartOfTime: EventTime = Long.MinValue
  val EndOfTime: EventTime = Long.MaxValue

  type Sequence = Int // Break ties when multiple groups of events are processed in the same EventTime

  type PropertiesAtTime = Map[String, PropertyValueType] // Collapsed properties at some point in time

  // TODO: Enrich this type: direction/direction-less
  case class Edge(k: String, other: NodeId)

  type EdgesAtTime = Set[Edge] // Collapsed properties at some point in time

  case class NodeStateAtTime(
    eventTime: EventTime,
    properties: PropertiesAtTime,
    edges: EdgesAtTime)
  
  trait Node {
    def id: NodeId

    def version: Int // This is the same as eventsAtTime.length
    def latest: EventTime // This is the same as eventsAtTime.lastOption.getOrElse(StartOfTime)

    // TODO: Implement these in various ways - remove PackedNode
    // def eventsAtTime: Vector[EventsAtTime]
    
    // def packed: Array[Byte]
    
    def current: IO[UnpackFailure, NodeStateAtTime]
    def atTime(atTime: EventTime): IO[UnpackFailure, NodeStateAtTime]

    def isCurrentlyEmpty: IO[UnpackFailure, Boolean]
    def wasAlwaysEmpty: IO[UnpackFailure, Boolean]
  }
}
