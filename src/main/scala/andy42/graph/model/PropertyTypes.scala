package andy42.graph.model

import java.time.Instant

type ScalarType = Unit | Boolean | Long | Double | String | BinaryValue | Instant

final case class BinaryValue(value: Vector[Byte])

// The structured types can only contain scalar types (i.e., no recursive structured types)
final case class PropertyArrayValue(value: Vector[ScalarType])
final case class PropertyMapValue(value: Map[String, ScalarType])

type PropertyValueType = ScalarType | PropertyArrayValue | PropertyMapValue

type PropertySnapshot = Map[String, PropertyValueType] // Collapsed properties at some point in time

object PropertySnapshot:
  val empty = Map.empty[String, PropertyValueType]
