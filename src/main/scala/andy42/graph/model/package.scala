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

  case class NodeStateAtTime(
    eventTime: EventTime,
    properties: PropertiesAtTime,
    edges: EdgesAtTime)

  type PackedNodeContents = Array[Byte]
  
  trait Node {
    def id: NodeId

    def version: Int
    def latest: EventTime

    def eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]]
    def packed: Array[Byte]
    
    lazy val current: IO[UnpackFailure, NodeStateAtTime] =
      for {
        eventsAtTime <- eventsAtTime
      } yield CollapseNodeHistory(eventsAtTime, latest)

    def atTime(atTime: EventTime): IO[UnpackFailure, NodeStateAtTime] =
      if (atTime >= latest)
        current
      else
        for {
          eventsAtTime <- eventsAtTime
        } yield CollapseNodeHistory(eventsAtTime, atTime)

    def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
      for {
        x <- current
      } yield x.properties.isEmpty && x.edges.isEmpty

    def wasAlwaysEmpty: IO[UnpackFailure, Boolean]
  }
}
