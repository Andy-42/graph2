package andy42.graph.model

import org.msgpack.core.*
import zio.*

import java.time.Instant


type ScalarType = Unit | Boolean | Int | Long | // Integral types will be promoted to Long
  Float | Double | // Floating point types will be promoted to Double
  String | BinaryValue | Instant

final case class BinaryValue(value: Vector[Byte])

final case class PropertyArrayValue(value: Vector[ScalarType])
final case class PropertyMapValue(value: Map[String, ScalarType])

type PropertyValueType = ScalarType | PropertyArrayValue | PropertyMapValue

type PropertySnapshot = Map[String, PropertyValueType] // Collapsed properties at some point in time

object PropertySnapshot:
  val empty = Map.empty[String, PropertyValueType]
