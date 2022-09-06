package andy42.graph

import java.time.Instant
import zio.IO
import scala.util.hashing.MurmurHash3

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
  case class Edge(k: String, other: NodeId) {
    def reverse(id: NodeId): Edge = copy(other = id)

    def edgeHash(id: NodeId): Long =
      MurmurHash3.stringHash(k) ^
       MurmurHash3.arrayHash(id.toArray) ^
       MurmurHash3.arrayHash(other.toArray)
  }

  type EdgesAtTime = Set[Edge] // Collapsed properties at some point in time
}
