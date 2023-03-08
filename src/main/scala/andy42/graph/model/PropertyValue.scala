package andy42.graph.model

import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.ValueType
import zio.*

import java.io.IOException
import java.time.Instant

import UnpackOperations.unpackCountedToSeq

object PropertyValue extends Unpackable[PropertyValueType]:

  private def unpackScalar(using unpacker: MessageUnpacker): IO[UnpackFailure, ScalarType] =
    UnpackSafely {
      unpacker.getNextFormat.getValueType match
        case ValueType.NIL     => ZIO.attempt(unpacker.unpackNil())
        case ValueType.BOOLEAN => ZIO.attempt(unpacker.unpackBoolean())
        case ValueType.INTEGER => ZIO.attempt(unpacker.unpackLong())
        case ValueType.FLOAT   => ZIO.attempt(unpacker.unpackDouble())
        case ValueType.STRING  => ZIO.attempt(unpacker.unpackString())
        case ValueType.BINARY =>
          ZIO.attempt {
            val length = unpacker.unpackBinaryHeader()
            BinaryValue(unpacker.readPayload(length).toVector)
          }

        case ValueType.EXTENSION => ZIO.attempt(unpacker.unpackTimestamp())

        case valueType: ValueType => ZIO.fail(UnexpectedScalarValueType(valueType))
    }

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, PropertyValueType] =
    UnpackSafely {
      unpacker.getNextFormat.getValueType match
        case ValueType.NIL       => ZIO.attempt(unpacker.unpackNil())
        case ValueType.BOOLEAN   => ZIO.attempt(unpacker.unpackBoolean())
        case ValueType.INTEGER   => ZIO.attempt(unpacker.unpackLong())
        case ValueType.FLOAT     => ZIO.attempt(unpacker.unpackDouble())
        case ValueType.STRING    => ZIO.attempt(unpacker.unpackString())
        case ValueType.EXTENSION => ZIO.attempt(unpacker.unpackTimestamp())
        case ValueType.BINARY =>
          ZIO.attempt {
            val length = unpacker.unpackBinaryHeader()
            BinaryValue(unpacker.readPayload(length).toVector)
          }

        case ValueType.ARRAY =>
          for
            length <- ZIO.attempt(unpacker.unpackArrayHeader())
            v <- unpackCountedToSeq(unpackScalar, length)
          yield PropertyArrayValue(v.toVector)

        case ValueType.MAP =>
          for
            length <- ZIO.attempt(unpacker.unpackMapHeader())
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
        v.foreach { case (k, v) => packer.packString(k); pack(v) }
        packer

  def unpackKeyValue(using unpacker: MessageUnpacker): IO[UnpackFailure | Throwable, (String, ScalarType)] =
    for
      k <- ZIO.attempt(unpacker.unpackString())
      v <- unpackScalar
    yield k -> v
