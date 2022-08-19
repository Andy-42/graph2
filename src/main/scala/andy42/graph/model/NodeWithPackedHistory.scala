package andy42.graph.model

import zio.{IO, ZIO}
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessagePack
import andy42.graph.model.PackedNode

case class NodeWithPackedHistory(
    id: NodeId,
    version: Int,
    latest: EventTime,
    packed: PackedNodeContents
) extends Node with PackedNode {

  lazy val nodeEventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  override lazy val current: IO[UnpackFailure, NodeStateAtTime] =
    for {
      eventsAtTime <- nodeEventsAtTime
    } yield CollapseNodeHistory(eventsAtTime, latest)

  override def atTime(atTime: EventTime): IO[UnpackFailure, NodeStateAtTime] =
    if (atTime >= latest)
      current
    else
      for {
        eventsAtTime <- nodeEventsAtTime
      } yield CollapseNodeHistory(eventsAtTime, atTime)

  override def isEmpty: IO[UnpackFailure, Boolean] =
    for {
      x <- current
    } yield x.properties.isEmpty && x.edges.isEmpty

  override def wasAlwaysEmpty: IO[UnpackFailure, Boolean] =
    ZIO.succeed(packed.isEmpty)
}

object Node {
  
  def apply(id: NodeId): Node with PackedNode =
     NodeWithPackedHistory(
      id = id,
      version = 0,
      latest = StartOfTime,
      packed = Array.empty[Byte]
    )

  def apply(
      id: NodeId,
      eventsAtTime: Vector[EventsAtTime]
  ): Node with PackedNode =
    NodeWithPackedHistory(
      id = id,
      version = eventsAtTime.length,
      latest = eventsAtTime.last.eventTime,
      packed = EventHistory.packToArray(eventsAtTime)
    )

  def apply(
      id: NodeId,
      version: Int,
      latest: EventTime,
      packed: PackedNodeContents
      ): Node with PackedNode =
        NodeWithPackedHistory(
          id = id,
          version = version,
          latest = latest,
          packed = packed
        )

}
