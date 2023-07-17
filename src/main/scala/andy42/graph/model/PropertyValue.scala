package andy42.graph.model

import andy42.graph.model.UnpackOperations.unpackCountedToSeq
import org.msgpack.core.{MessagePacker, MessageUnpacker}
import org.msgpack.value.ValueType
import zio.*

import java.io.IOException
import java.time.Instant

object PropertyValue extends Unpackable[PropertyValueType]:

  private def unpackScalar(using unpacker: MessageUnpacker): IO[UnpackFailure, ScalarType] =
    unpacker.getNextFormat.getValueType match {
      case ValueType.NIL     => UnpackSafely { unpacker.unpackNil() }
      case ValueType.BOOLEAN => UnpackSafely { unpacker.unpackBoolean() }
      case ValueType.INTEGER => UnpackSafely { unpacker.unpackLong() }
      case ValueType.FLOAT   => UnpackSafely { unpacker.unpackDouble() }
      case ValueType.STRING  => UnpackSafely { unpacker.unpackString() }
      case ValueType.BINARY =>
        UnpackSafely {
          val length = unpacker.unpackBinaryHeader()
          BinaryValue(unpacker.readPayload(length).toVector)

        }

      case ValueType.EXTENSION => UnpackSafely { unpacker.unpackTimestamp() }

      case valueType: ValueType => ZIO.fail(UnexpectedScalarValueType(valueType))
    }

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, PropertyValueType] =
    unpacker.getNextFormat.getValueType match {
      case ValueType.NIL       => UnpackSafely { unpacker.unpackNil() }
      case ValueType.BOOLEAN   => UnpackSafely { unpacker.unpackBoolean() }
      case ValueType.INTEGER   => UnpackSafely { unpacker.unpackLong() }
      case ValueType.FLOAT     => UnpackSafely { unpacker.unpackDouble() }
      case ValueType.STRING    => UnpackSafely { unpacker.unpackString() }
      case ValueType.EXTENSION => UnpackSafely { unpacker.unpackTimestamp() }
      case ValueType.BINARY =>
        UnpackSafely {
          val length = unpacker.unpackBinaryHeader()
          BinaryValue(unpacker.readPayload(length).toVector)
        }

      case ValueType.ARRAY =>
        for
          length <- UnpackSafely { unpacker.unpackArrayHeader() }
          v <- unpackCountedToSeq(unpackScalar, length)
        yield PropertyArrayValue(v.toVector)

      case ValueType.MAP =>
        for
          length <- UnpackSafely { unpacker.unpackMapHeader() }
          m <- unpackCountedToSeq(unpackKeyValue, length)
        yield PropertyMapValue(m.toMap)
    }

  def pack(value: PropertyValueType)(using packer: MessagePacker): MessagePacker =
    value match
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
        packer

      case PropertyMapValue(v) =>
        packer.packMapHeader(v.size)
        v.foreach { (k, v) => packer.packString(k); pack(v) }
        packer

  def unpackKeyValue(using unpacker: MessageUnpacker): IO[UnpackFailure, (String, ScalarType)] =
    for
      k <- UnpackSafely { unpacker.unpackString() }
      v <- unpackScalar
    yield k -> v
