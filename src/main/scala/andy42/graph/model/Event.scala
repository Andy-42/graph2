package andy42.graph.model

import andy42.graph.model.*
import org.msgpack.core.*
import zio.*

import java.io.IOException
import andy42.graph.services.EdgeReconciliationEvent.apply
import andy42.graph.services.EdgeReconciliationEvent

enum Event extends Packable:
  case NodeRemoved
  case PropertyAdded(k: String, value: PropertyValueType)
  case PropertyRemoved(k: String)
  case EdgeAdded(edge: Edge)
  case EdgeRemoved(edge: Edge)
  case FarEdgeAdded(edge: Edge)
  case FarEdgeRemoved(edge: Edge)

  override def pack(using packer: MessagePacker): Unit =
    packer.packInt(ordinal)

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

      case FarEdgeAdded(edge) =>
        packer.packInt(Event.farEdgeAddedOrdinal)
        edge.pack

      case FarEdgeRemoved(edge) =>
        packer.packInt(Event.farEdgeRemovedOrdinal)
        edge.pack

object Event extends Unpackable[Event]:

  val nodeRemovedOrdinal = 0
  val propertyAddedOrdinal = 1
  val propertyRemovedOrdinal = 2
  val edgeAddedOrdinal = 3
  val edgeRemovedOrdinal = 4
  val farEdgeAddedOrdinal = 5
  val farEdgeRemovedOrdinal = 6

  override def unpack(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    unpacker.unpackInt() match
      case Event.nodeRemovedOrdinal     => ZIO.succeed(NodeRemoved)
      case Event.propertyAddedOrdinal   => unpackPropertyAdded
      case Event.propertyRemovedOrdinal => unpackPropertyRemoved
      case Event.edgeAddedOrdinal       => unpackEdgeAdded(isFar = false)
      case Event.edgeRemovedOrdinal     => unpackEdgeRemoved(isFar = false)
      case Event.farEdgeAddedOrdinal    => unpackEdgeAdded(isFar = true)
      case Event.farEdgeRemovedOrdinal  => unpackEdgeRemoved(isFar = true)

      case unexpectedEventType =>
        ZIO.fail(UnexpectedEventDiscriminator(unexpectedEventType))

  def unpackPropertyAdded(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    UnpackSafely {
      for
        k <- ZIO.attempt(unpacker.unpackString())
        value <- PropertyValue.unpack
      yield PropertyAdded(k, value)
    }

  def unpackPropertyRemoved(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    UnpackSafely {
      for k <- ZIO.attempt(unpacker.unpackString()) yield PropertyRemoved(k)
    }

  def unpackEdgeAdded(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- Edge.unpack
    yield if isFar then FarEdgeAdded(edge) else EdgeAdded(edge)

  def unpackEdgeRemoved(
      isFar: Boolean
  )(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- Edge.unpack
    yield if isFar then FarEdgeRemoved(edge) else EdgeRemoved(edge)

object Events extends CountedSeqPacker[Event]:

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[Event]] =
    UnpackSafely {
      given unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(packed)

      for
        length <- ZIO.attempt(unpacker.unpackInt())
        events <- UnpackOperations.unpackCountedToSeq(Event.unpack, length)
      yield events.toVector
    }
