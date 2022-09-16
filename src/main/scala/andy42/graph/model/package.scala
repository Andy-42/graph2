package andy42.graph.model

import java.time.Instant
import zio.IO
import scala.util.hashing.MurmurHash3

type ScalarType = Unit | Boolean | Int | Long | // Integral types will be promoted to Long
  Float | Double | // Floating point types will be promoted to Double
  String | BinaryValue | Instant

final case class BinaryValue(value: Vector[Byte])
final case class PropertyArrayValue(value: Vector[ScalarType])
final case class PropertyMapValue(value: Map[String, ScalarType])

type PropertyValueType = ScalarType | PropertyArrayValue | PropertyMapValue

type NodeId = Vector[Byte] // Always 8 bytes

// The time an event occurs
type EventTime = Long // Epoch Millis
val StartOfTime: EventTime = Long.MinValue
val EndOfTime: EventTime = Long.MaxValue

type Sequence = Int // Break ties when multiple groups of events are processed in the same EventTime

type PropertySnapshot = Map[String, PropertyValueType] // Collapsed properties at some point in time

object PropertySnapshot:
  val empty = Map.empty[String, PropertyValueType]

// TODO: Enrich this type: direction/direction-less
final case class Edge(k: String, other: NodeId):

  def reverse(id: NodeId): Edge = copy(other = id)

  /** A hash of the edge that can be used to test eventual consistency of the far edge state.
    *
    * forall(x => x ^ x == 0)
    *
    * This works even with a larger aggregation there are many values being included in no particular order. As long as
    * each item in is ^-ed into the aggregate twice. Each edge's hash will include the the key, and the ids at either
    * end once.
    *
    * forall(edge => (edge.hash(id) ^ edge.reverse(id).hash(edge.other) == 0)
    *
    * @param id
    *   The id of the node containing this half-edge.
    * @return
    *   A hash of the edge that will reconcile to zero when ^-ed with the hash of the edge reversed.
    */
  def hash(id: NodeId): EdgeHash =
    MurmurHash3.stringHash(k) ^
      MurmurHash3.arrayHash(id.toArray) ^
      MurmurHash3.arrayHash(other.toArray)

type EdgeSnapshot = Set[Edge] // Collapsed properties at some point in time

object EdgeSnapshot:
  val empty = Set.empty[Edge]

type EdgeHash = Long // Correctly balanced edges will reconcile to zero
