package andy42.graph.model

import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessagePacker
import org.msgpack.value.ValueType
import zio._
import java.time.Instant

object PropertyValue {

  private def unpackScalar(implicit unpacker: MessageUnpacker): Task[ScalarType] =
    unpacker.getNextFormat.getValueType match {
      case ValueType.NIL     => ZIO.unit
      case ValueType.BOOLEAN => ZIO.attempt { unpacker.unpackBoolean() }
      case ValueType.INTEGER => ZIO.attempt { unpacker.unpackLong() }
      case ValueType.FLOAT   => ZIO.attempt { unpacker.unpackDouble() }
      case ValueType.STRING  => ZIO.attempt { unpacker.unpackString() }
      case ValueType.BINARY =>
        ZIO.attempt {
          val length = unpacker.unpackBinaryHeader()
          BinaryValue(unpacker.readPayload(length).toVector)
        }

      case ValueType.EXTENSION => ZIO.attempt { unpacker.unpackTimestamp() }

      case _ => ZIO.fail(new IllegalStateException("Unexpected value type"))
    }

  def unpack(implicit
      unpacker: MessageUnpacker
  ): Task[PropertyValueType] =
    unpacker.getNextFormat.getValueType match {
      case ValueType.NIL       => ZIO.unit
      case ValueType.BOOLEAN   => ZIO.attempt { unpacker.unpackBoolean() }
      case ValueType.INTEGER   => ZIO.attempt { unpacker.unpackLong() }
      case ValueType.FLOAT     => ZIO.attempt { unpacker.unpackDouble() }
      case ValueType.STRING    => ZIO.attempt { unpacker.unpackString() }
      case ValueType.EXTENSION => ZIO.attempt { unpacker.unpackTimestamp() }
      case ValueType.BINARY =>
        ZIO.attempt {
          val length = unpacker.unpackBinaryHeader()
          BinaryValue(unpacker.readPayload(length).toVector)
        }

      case ValueType.ARRAY =>
        for {
          length <- ZIO.attempt { unpacker.unpackArrayHeader() }
          v <- unpackToVector(length)
        } yield PropertyArrayValue(v)

      case ValueType.MAP =>
        for {
          length <- ZIO.attempt { unpacker.unpackMapHeader() }
          m <- unpackToMap(length)
        } yield PropertyMapValue(m)
    }

  def pack(value: PropertyValueType)(implicit packer: MessagePacker): Unit =
    value match {
      case ()         => packer.packNil()
      case v: Boolean => packer.packBoolean(v)
      case v: Int     => packer.packInt(v)
      case v: Long    => packer.packLong(v)
      case v: Float   => packer.packFloat(v)
      case v: Double  => packer.packDouble(v)
      case v: String  => packer.packString(v)
      case v: Instant => packer.packTimestamp(v)

      case BinaryValue(v) =>
        packer.packBinaryHeader(v.length)
        packer.addPayload(v.toArray)

      case PropertyArrayValue(v) =>
        packer.packArrayHeader(v.size)
        v.foreach(pack)

      case PropertyMapValue(v) =>
        packer.packMapHeader(v.size)
        v.foreach { case (k, v) => packer.packString(k); pack(v) }
    }

  private def unpackToVector(
      length: Int
  )(implicit unpacker: MessageUnpacker): Task[Vector[ScalarType]] = {
    val a = Array.ofDim[ScalarType](length)

    def accumulate(i: Int = 0): Task[Vector[ScalarType]] =
      if (i == length)
        ZIO.succeed(a.toVector)
      else
        unpackScalar.flatMap { t =>
          a(i) = t
          accumulate(i + 1)
        }

    accumulate()
  }

  private def unpackToMap(
      length: Int
  )(implicit
      unpacker: MessageUnpacker
  ): Task[Map[String, ScalarType]] = {
    val a = Array.ofDim[(String, ScalarType)](length)

    def nextKV(implicit
        unpacker: MessageUnpacker
    ): Task[(String, ScalarType)] =
      for {
        k <- ZIO.attempt { unpacker.unpackString() }
        v <- unpackScalar
      } yield k -> v

    def accumulate(i: Int = 0): Task[Map[String, ScalarType]] =
      if (i == length)
        ZIO.succeed(a.toMap)
      else
        nextKV.flatMap { t =>
          a(i) = t
          accumulate(i + 1)
        }

    accumulate()
  }
}
