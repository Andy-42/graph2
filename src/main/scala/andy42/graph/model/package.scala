package andy42.graph

import java.time.Instant
import zio.Task

package object model {

  type ScalarType = Unit | 
    Boolean | 
    Int | Long | 
    Float | Double |
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

  case class Edge(k: String, other: NodeId)

  type EdgesAtTime = Set[Edge] // Collapsed properties at some point in time

  type PackedNode = Array[Byte]

  case class NodeStateAtTime(
    eventTime: EventTime,
    properties: PropertiesAtTime,
    edges: EdgesAtTime)

  trait Node {
    def id: NodeId
    def version: Int
    
    def current: Task[NodeStateAtTime]
    def atTime(atTime: EventTime): Task[NodeStateAtTime]
    
    def packed: PackedNode

    def isEmpty: Task[Boolean]
    def wasAlwaysEmpty: Task[Boolean]
  }
}
