package andy42.graph.model

import zio._
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessagePack

case class NodeFromEventsAtTime(
    id: NodeId,
    version: Int,
    latest: EventTime,
    unpackedEventsAtTime: Vector[EventsAtTime]
) extends Node {

  override def eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    ZIO.succeed(unpackedEventsAtTime)

  override def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
    if (unpackedEventsAtTime.isEmpty)
      ZIO.succeed(true)
    else
      super.isCurrentlyEmpty

  override def wasAlwaysEmpty: IO[UnpackFailure, Boolean] =
    ZIO.succeed(unpackedEventsAtTime.isEmpty)

  override def packed: Array[Byte] =
    EventHistory.packToArray(unpackedEventsAtTime)
}

case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    latest: EventTime,
    packed: PackedNodeContents
) extends Node {

  override lazy val eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  override def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
    if (packed.isEmpty)
      ZIO.succeed(true)
    else
      super.isCurrentlyEmpty

  override def wasAlwaysEmpty: IO[UnpackFailure, Boolean] =
    ZIO.succeed(packed.isEmpty)
}

object Node {

  def apply(id: NodeId): Node =
    new Node {
      val id: NodeId = id
      def version: Int = 0
      def latest: EventTime = StartOfTime
      def packed: PackedNodeContents = Array.empty[Byte]
      def eventsAtTime: UIO[Vector[EventsAtTime]] = ZIO.succeed(Vector.empty)
      override def isCurrentlyEmpty: UIO[Boolean] = ZIO.succeed(true)
      def wasAlwaysEmpty: UIO[Boolean] = ZIO.succeed(true)
    }

  def apply(
      id: NodeId,
      eventsAtTime: Vector[EventsAtTime]
  ): Node =
    NodeFromEventsAtTime(
      id = id,
      version = eventsAtTime.length,
      latest = eventsAtTime.lastOption.fold(StartOfTime)(_.eventTime),
      unpackedEventsAtTime = eventsAtTime
    )

  def apply(
      id: NodeId,
      version: Int,
      latest: EventTime,
      packed: PackedNodeContents
  ): Node =
    NodeFromPackedHistory(
      id = id,
      version = version,
      latest = latest,
      packed = packed
    )
}
