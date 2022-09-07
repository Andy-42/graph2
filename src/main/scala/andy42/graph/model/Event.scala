package andy42.graph.model

import andy42.graph.model.PropertyValue
import andy42.graph.model.UnpackOperations.unpackToVector
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker
import org.msgpack.core.MessageUnpacker
import zio._

import java.io.IOException

object EventType {
  val NODE_REMOVED: Byte = 0.toByte
  val PROPERTY_ADDED: Byte = 1.toByte
  val PROPERTY_REMOVED: Byte = 2.toByte
  val EDGE_ADDED: Byte = 3.toByte
  val EDGE_REMOVED: Byte = 4.toByte
  val FAR_EDGE_ADDED: Byte = 5.toByte
  val FAR_EDGE_REMOVED: Byte = 6.toByte
}

sealed trait Event extends Packable {
  override def pack(using packer: MessagePacker): MessagePacker
}

type NearEdgeEvent = EdgeAdded | EdgeRemoved
type FarEdgeEvent = FarEdgeAdded | FarEdgeRemoved

object Event extends Unpackable[Event] {

  override def unpack(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, Event] = {
    unpacker.unpackByte() match
      case EventType.NODE_REMOVED     => ZIO.succeed(NodeRemoved)
      case EventType.PROPERTY_ADDED   => unpackPropertyAdded
      case EventType.PROPERTY_REMOVED => unpackPropertyRemoved
      case EventType.EDGE_ADDED       => unpackEdgeAdded(isFar = false)
      case EventType.EDGE_REMOVED     => unpackEdgeRemoved(isFar = false)
      case EventType.FAR_EDGE_ADDED   => unpackEdgeAdded(isFar = true)
      case EventType.FAR_EDGE_REMOVED => unpackEdgeRemoved(isFar = true)

      case unexpectedEventType: Byte =>
        ZIO.fail(UnexpectedDiscriminator(unexpectedEventType, "EventType"))
  }.refineOrDie(UnpackFailure.refine)

  def unpackPropertyAdded(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, PropertyAdded] = {
    for
      k <- ZIO.attempt { unpacker.unpackString() }
      value <- PropertyValue.unpack
    yield PropertyAdded(k, value)
  }.refineOrDie(UnpackFailure.refine)

  def unpackPropertyRemoved(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, PropertyRemoved] = {
    for
      k <- ZIO.attempt { unpacker.unpackString() }
    yield PropertyRemoved(k)
  }.refineOrDie(UnpackFailure.refine)

  def unpackEdgeAdded(isFar: Boolean)(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, EdgeAdded | FarEdgeAdded] =
    for edge <- unpackEdge
    yield if isFar then FarEdgeAdded(edge) else EdgeAdded(edge)

  def unpackEdgeRemoved(isFar: Boolean)(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, EdgeRemoved | FarEdgeRemoved] =
    for edge <- unpackEdge
    yield if isFar then FarEdgeRemoved(edge) else EdgeRemoved(edge)

  private def unpackEdge(using
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, Edge] = {
    for
      k <- ZIO.attempt { unpacker.unpackString() }
      length <- ZIO.attempt { unpacker.unpackBinaryHeader() }
      other <- ZIO.attempt { unpacker.readPayload(length).toVector }
    yield Edge(k, other)
  }.refineOrDie(UnpackFailure.refine)
}

case object NodeRemoved extends Event {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer.packByte(EventType.NODE_REMOVED)
}

final case class PropertyAdded(k: String, value: PropertyValueType) extends Event {

  override def pack(using packer: MessagePacker): MessagePacker = {
    packer
      .packByte(EventType.PROPERTY_ADDED)
      .packString(k)

    PropertyValue.pack(value)
    packer
  }
}

final case class PropertyRemoved(k: String) extends Event {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.PROPERTY_REMOVED)
      .packString(k)
}

trait EdgeEvent {
  def edge: Edge
}

final case class EdgeAdded(edge: Edge) extends Event with EdgeEvent {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.EDGE_ADDED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}

final case class FarEdgeAdded(edge: Edge) extends Event with EdgeEvent {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.FAR_EDGE_ADDED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}

final case class EdgeRemoved(edge: Edge) extends Event with EdgeEvent {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.EDGE_REMOVED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}

final case class FarEdgeRemoved(edge: Edge) extends Event with EdgeEvent {

  override def pack(using packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.FAR_EDGE_REMOVED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}

object Events {

  def pack(events: Vector[Event]): Array[Byte] = {

    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()

    packer.packInt(events.length)
    events.foreach(_.pack)
    packer.toByteArray()
  }

  def unpack(packed: Array[Byte]): IO[UnpackFailure, Vector[Event]] = {

    given unpacker: MessageUnpacker = MessagePack.newDefaultUnpacker(packed)

    for
       length <- ZIO.attempt { unpacker.unpackInt() }
       events <- unpackToVector(Event.unpack, length)
    yield events
  }.refineOrDie(UnpackFailure.refine)
}