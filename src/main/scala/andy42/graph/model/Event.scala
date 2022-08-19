package andy42.graph.model

import org.msgpack.core.{MessagePacker, MessageUnpacker}
import andy42.graph.model.PropertyValue

import java.io.IOException

import zio.{IO, ZIO}

object EventType {
  val NODE_REMOVED: Byte = 0.toByte
  val PROPERTY_ADDED: Byte = 1.toByte
  val PROPERTY_REMOVED: Byte = 2.toByte
  val EDGE_ADDED: Byte = 3.toByte
  val EDGE_REMOVED: Byte = 4.toByte
}

// TODO: Re-introduce the FAR_EDGE_* events

sealed trait Event extends Packable {
  override def pack(implicit packer: MessagePacker): MessagePacker
}

object Event extends Unpackable[Event] {

  override def unpack(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, Event] = {
    unpacker.unpackByte() match {
      case EventType.NODE_REMOVED     => ZIO.succeed(NodeRemoved)
      case EventType.PROPERTY_ADDED   => unpackPropertyAdded
      case EventType.PROPERTY_REMOVED => unpackPropertyRemoved
      case EventType.EDGE_ADDED       => unpackEdgeAdded
      case EventType.EDGE_REMOVED     => unpackEdgeRemoved

      case unexpectedEventType: Byte =>
        ZIO.fail(UnexpectedDiscriminator(unexpectedEventType, "EventType"))
    }
  }.refineOrDie(UnpackFailure.refine)

  def unpackPropertyAdded(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, PropertyAdded] = {
    for {
      k <- ZIO.attempt { unpacker.unpackString() }
      value <- PropertyValue.unpack
    } yield PropertyAdded(k, value)
  }.refineOrDie(UnpackFailure.refine)

  def unpackPropertyRemoved(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, PropertyRemoved] = {
    for {
      k <- ZIO.attempt { unpacker.unpackString() }
    } yield PropertyRemoved(k)
  }.refineOrDie(UnpackFailure.refine)

  def unpackEdgeAdded(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, EdgeAdded] = {
    for {
      k <- ZIO.attempt { unpacker.unpackString() }
      length <- ZIO.attempt { unpacker.unpackBinaryHeader() }
      other <- ZIO.attempt { unpacker.readPayload(length).toVector }
    } yield EdgeAdded(Edge(k, other))
  }.refineOrDie(UnpackFailure.refine)

  def unpackEdgeRemoved(implicit
      unpacker: MessageUnpacker
  ): IO[UnpackFailure, EdgeRemoved] = {
    for {
      k <- ZIO.attempt { unpacker.unpackString() }
      length <- ZIO.attempt { unpacker.unpackBinaryHeader() }
      other <- ZIO.attempt { unpacker.readPayload(length).toVector }
    } yield EdgeRemoved(Edge(k, other))
  }.refineOrDie(UnpackFailure.refine)
}

case object NodeRemoved extends Event {

  override def pack(implicit packer: MessagePacker): MessagePacker =
    packer.packByte(EventType.NODE_REMOVED)
}

case class PropertyAdded(k: String, value: PropertyValueType) extends Event {

  override def pack(implicit packer: MessagePacker): MessagePacker = {
    packer
      .packByte(EventType.PROPERTY_ADDED)
      .packString(k)

    PropertyValue.pack(value)
    packer
  }
}

case class PropertyRemoved(k: String) extends Event {

  override def pack(implicit packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.PROPERTY_REMOVED)
      .packString(k)
}

case class EdgeAdded(edge: Edge) extends Event {

  override def pack(implicit packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.EDGE_ADDED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}

case class EdgeRemoved(edge: Edge) extends Event {

  override def pack(implicit packer: MessagePacker): MessagePacker =
    packer
      .packByte(EventType.EDGE_REMOVED)
      .packString(edge.k)
      .packBinaryHeader(edge.other.length)
      .writePayload(edge.other.to(Array))
}
