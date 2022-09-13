package andy42.graph.model

import andy42.graph.model.PropertyValue
import andy42.graph.model.UnpackOperations.unpackCountedToVector
import org.msgpack.core._
import zio._

import java.io.IOException

enum Event extends Packable:
  case NodeRemoved
  case PropertyAdded(k: String, value: PropertyValueType)
  case PropertyRemoved(k: String)
  case EdgeAdded(edge: Edge)
  case EdgeRemoved(edge: Edge)
  case FarEdgeAdded(edge: Edge)
  case FarEdgeRemoved(edge: Edge)

  override def pack(using packer: MessagePacker): MessagePacker = this match
    case NodeRemoved =>
      packer.packInt(ordinal)

    case PropertyAdded(k, value) =>
      packer
        .packInt(ordinal)
        .packString(k)
      PropertyValue.pack(value)

    case PropertyRemoved(k) =>
      packer
        .packInt(ordinal)
        .packString(k)

    case EdgeAdded(edge) =>
      packer
        .packInt(ordinal)
        .packString(edge.k)
        .packBinaryHeader(edge.other.length)
        .writePayload(edge.other.to(Array))

    case EdgeRemoved(edge) =>
      packer
        .packInt(ordinal)
        .packString(edge.k)
        .packBinaryHeader(edge.other.length)
        .writePayload(edge.other.to(Array))

    case FarEdgeAdded(edge) =>
      packer
        .packInt(ordinal)
        .packString(edge.k)
        .packBinaryHeader(edge.other.length)
        .writePayload(edge.other.to(Array))

    case FarEdgeRemoved(edge: Edge) =>
      packer
        .packInt(ordinal)
        .packString(edge.k)
        .packBinaryHeader(edge.other.length)
        .writePayload(edge.other.to(Array))

  def edgeAffected: Option[Edge] = this match
    case EdgeAdded(edge)      => Some(edge)
    case EdgeRemoved(edge)    => Some(edge)
    case FarEdgeAdded(edge)   => Some(edge)
    case FarEdgeRemoved(edge) => Some(edge)
    case _                    => None

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

  def unpackPropertyAdded(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] = {
    for
      k <- ZIO.attempt(unpacker.unpackString())
      value <- PropertyValue.unpack
    yield PropertyAdded(k, value)
  }.refineOrDie(UnpackFailure.refine)

  def unpackPropertyRemoved(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] = {
    for k <- ZIO.attempt(unpacker.unpackString()) yield PropertyRemoved(k)
  }.refineOrDie(UnpackFailure.refine)

  def unpackEdgeAdded(isFar: Boolean)(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- unpackEdge
    yield if isFar then FarEdgeAdded(edge) else EdgeAdded(edge)

  def unpackEdgeRemoved(
      isFar: Boolean
  )(using unpacker: MessageUnpacker): IO[UnpackFailure, Event] =
    for edge <- unpackEdge
    yield if isFar then FarEdgeRemoved(edge) else EdgeRemoved(edge)

  private def unpackEdge(using unpacker: MessageUnpacker): IO[UnpackFailure, Edge] = {
    for
      k <- ZIO.attempt(unpacker.unpackString())
      length <- ZIO.attempt(unpacker.unpackBinaryHeader())
      other <- ZIO.attempt(unpacker.readPayload(length).toVector)
    yield Edge(k, other)
  }.refineOrDie(UnpackFailure.refine)

object Events extends CountedSeqPacker[Event]:

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[Event]] = {

    given unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(packed)

    for
      length <- ZIO.attempt(unpacker.unpackInt())
      events <- unpackCountedToVector(Event.unpack, length)
    yield events
  }.refineOrDie(UnpackFailure.refine)
