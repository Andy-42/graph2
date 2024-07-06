package andy42.graph.model

import andy42.graph.model.*
import org.msgpack.core.*
import zio.*

enum Event extends Packable:
  case NodeRemoved

  case PropertyAdded(k: String, value: PropertyValueType)
  case PropertyRemoved(k: String)

  case EdgeAdded(edge: Edge)
  case EdgeRemoved(edge: Edge)

  override def pack(using packer: MessagePacker): Unit =
    this match
      case NodeRemoved =>
        packer.packInt(Event.nodeRemovedOrdinal)

      case PropertyAdded(k, value) =>
        packer.packInt(Event.propertyAddedOrdinal)
        packer.packString(k)
        PropertyValue.pack(value)

      case PropertyRemoved(k) =>
        packer.packInt(Event.propertyRemovedOrdinal)
        packer.packString(k)

      case EdgeAdded(edge) =>
        packer.packInt(Event.edgeAddedOrdinal)
        edge.pack

      case EdgeRemoved(edge) =>
        packer.packInt(Event.edgeRemovedOrdinal)
        edge.pack

object Event extends Unpackable[Event]:

  private val nodeRemovedOrdinal = 0
  private val propertyAddedOrdinal = 1
  private val propertyRemovedOrdinal = 2
  private val edgeAddedOrdinal = 3
  private val edgeRemovedOrdinal = 4

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    unpacker.unpackInt() match
      case Event.nodeRemovedOrdinal     => ZIO.succeed(NodeRemoved)
      case Event.propertyAddedOrdinal   => unpackPropertyAdded
      case Event.propertyRemovedOrdinal => unpackPropertyRemoved
      case Event.edgeAddedOrdinal       => unpackEdgeAdded
      case Event.edgeRemovedOrdinal     => unpackEdgeRemoved

      case unexpectedEventType =>
        ZIO.fail(UnexpectedEventDiscriminator(unexpectedEventType))

  private def unpackPropertyAdded(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for
      k <- UnpackSafely { unpacker.unpackString() }
      value <- PropertyValue.unpack
    yield PropertyAdded(k, value)

  private def unpackPropertyRemoved(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for k <- UnpackSafely { unpacker.unpackString() } yield PropertyRemoved(k)

  private def unpackEdgeAdded(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- Edge.unpack
    yield EdgeAdded(edge)

  private def unpackEdgeRemoved(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- Edge.unpack
    yield EdgeRemoved(edge)

object Events extends CountedSeqPacker[Event]:

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[Event]] =
    given unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(packed)

    for
      length <- UnpackSafely { unpacker.unpackInt() }
      events <- UnpackOperations.unpackCountedToSeq(Event.unpack, length)
    yield events.toVector

  def pack(events: Vector[Event]): Array[Byte] =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    packer.packInt(events.length)
    events.foreach(_.pack)
    packer.toByteArray
