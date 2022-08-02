package andy42.graph.model

import zio.{Task, ZIO}
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessagePack

case class NodeWithPackedHistory(id: NodeId,
                                 version: Int,
                                 packed: PackedNode
                                ) extends Node {

  lazy val nodeEventsAtTime: Task[Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  override lazy val current: Task[NodeStateAtTime] =
    for {
      eventsAtTime <- nodeEventsAtTime  
    } yield CollapseNodeHistory(eventsAtTime)

  override def atTime(atTime: EventTime): Task[NodeStateAtTime] =
    for {
      eventsAtTime <- nodeEventsAtTime
    } yield CollapseNodeHistory(eventsAtTime, atTime)  

  override def isEmpty: Task[Boolean] =
    for {
      x <- current
    } yield x.properties.isEmpty && x.edges.isEmpty
  
  override def wasAlwaysEmpty: Task[Boolean] = 
    ZIO.succeed(packed.isEmpty)
}
