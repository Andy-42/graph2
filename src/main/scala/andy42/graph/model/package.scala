package andy42.graph

import java.time.Instant

package object model {

  def foo(x: Int | Long): Long = ???

  // TODO: Render this as an enum

  type ValueType = Unit |
    Boolean | java.lang.Boolean | // java.lang.Boolean
    Int | Long | java.lang.Integer | java.lang.Long | // => java.lang.Long
    Float | Double | java.lang.Float | java.lang.Double | // => java.lang.Double
    String |
    Vector[Byte] |
    Instant

  type ContainerType = Vector[ValueType] | Map[String, ValueType]

  type PropertyValueType = ValueType | ContainerType


  // TODO: Do this as using new implicit mechanism
  def minimize(x: PropertyValueType): ValueType | ContainerType = x match {
    case v: Boolean => java.lang.Boolean.valueOf(v)
    case v: java.lang.Boolean => v
    case v: Int => java.lang.Integer.valueOf(v)
  }



}
