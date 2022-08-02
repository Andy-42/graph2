package andy42.graph.model

import org.msgpack.core.{MessagePacker, MessageUnpacker}
import andy42.graph.model.PropertyValue

import zio.{Task, ZIO}

object EventType {
  val NODE_REMOVED: Byte = 0.toByte
  val PROPERTY_ADDED: Byte = 1.toByte
  val PROPERTY_REMOVED: Byte = 2.toByte
  val EDGE_ADDED: Byte = 3.toByte
  val EDGE_REMOVED: Byte = 4.toByte
}

sealed trait Event extends Packable {
  override def pack(implicit packer: MessagePacker): MessagePacker
}

object Event extends Unpackable[Event] {

  override def unpack(implicit unpacker: MessageUnpacker): Task[Event] =
    unpacker.unpackByte() match {
      case EventType.NODE_REMOVED => ZIO.succeed(NodeRemoved)
      case EventType.PROPERTY_ADDED => unpackPropertyAdded
      case EventType.PROPERTY_REMOVED => unpackPropertyRemoved
      case EventType.EDGE_ADDED => unpackEdgeAdded
      case EventType.EDGE_REMOVED => unpackEdgeRemoved

      // TODO: case _ => ZIO.fail(...)
    }

  def unpackPropertyAdded(implicit unpacker: MessageUnpacker): Task[PropertyAdded] =
    for {
      k <- ZIO.attempt(unpacker.unpackString())
      value <- PropertyValue.unpack
    } yield PropertyAdded(k, value)

  def unpackPropertyRemoved(implicit unpacker: MessageUnpacker): Task[PropertyRemoved] =
    for {
      k <- ZIO.attempt(unpacker.unpackString())
    } yield PropertyRemoved(k)

  def unpackEdgeAdded(implicit unpacker: MessageUnpacker): Task[EdgeAdded] =
    for {
      k <- ZIO.attempt(unpacker.unpackString())
      length <- ZIO.attempt(unpacker.unpackBinaryHeader())
      other <- ZIO.attempt(unpacker.readPayload(length).toVector)
    } yield EdgeAdded(Edge(k, other))

  def unpackEdgeRemoved(implicit unpacker: MessageUnpacker): Task[EdgeRemoved] =
    for {
      k <- ZIO.attempt(unpacker.unpackString())
      length <- ZIO.attempt(unpacker.unpackBinaryHeader())
      other <- ZIO.attempt(unpacker.readPayload(length).toVector)
    } yield EdgeRemoved(Edge(k, other))
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
