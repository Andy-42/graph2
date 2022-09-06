package andy42.graph.model

import zio._
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessagePack

case class NodeStateAtTime(eventTime: EventTime, sequence: Int, properties: PropertiesAtTime, edges: EdgesAtTime)

type PackedNodeContents = Array[Byte]

sealed trait Node {
  def id: NodeId

  def version: Int
  def latestEventTime: EventTime
  def latestSequence: Int

  def eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]]
  def packed: Array[Byte]
  def append(events: Vector[Event], atTime: EventTime): Node

  lazy val current: IO[UnpackFailure, NodeStateAtTime] =
    for {
      eventsAtTime <- eventsAtTime
    } yield CollapseNodeHistory(eventsAtTime, latestEventTime)

  def atTime(atTime: EventTime): IO[UnpackFailure, NodeStateAtTime] =
    if (atTime >= latestEventTime)
      current
    else
      for {
        eventsAtTime <- eventsAtTime
      } yield CollapseNodeHistory(eventsAtTime, atTime)

  def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
    for {
      x <- current
    } yield x.properties.isEmpty && x.edges.isEmpty

  def wasAlwaysEmpty: Boolean
}

case class NodeFromEventsAtTime(
    id: NodeId,
    version: Int,
    latestEventTime: EventTime,
    latestSequence: Int,
    unpackedEventsAtTime: Vector[EventsAtTime]
) extends Node {

  override def append(events: Vector[Event], atTime: EventTime): Node = {
    require(atTime >= latestEventTime)

    val sequence = if (atTime > latestEventTime) 0 else latestSequence + 1

    copy(
      version = version + 1,
      latestEventTime = atTime,
      latestSequence = sequence,
      unpackedEventsAtTime = unpackedEventsAtTime :+ EventsAtTime(atTime, sequence, events)
    )
  }

  override def eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    ZIO.succeed(unpackedEventsAtTime)

  override def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
    if (unpackedEventsAtTime.isEmpty)
      ZIO.succeed(true)
    else
      super.isCurrentlyEmpty

  override def wasAlwaysEmpty: Boolean = unpackedEventsAtTime.isEmpty

  override def packed: Array[Byte] =
    EventHistory.packToArray(unpackedEventsAtTime)
}

case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    latestEventTime: EventTime,
    latestSequence: Int,
    packed: PackedNodeContents
) extends Node {

  override def append(events: Vector[Event], atTime: EventTime): Node = {
    require(atTime >= latestEventTime)

    val sequence = if (atTime > latestEventTime) 0 else latestSequence + 1

    copy(
      version = version + 1,
      latestEventTime = atTime,
      latestSequence = sequence,
      packed = packed ++ EventsAtTime(atTime, sequence, events).toByteArray
    )
  }

  override lazy val eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  override def isCurrentlyEmpty: IO[UnpackFailure, Boolean] =
    if (packed.isEmpty)
      ZIO.succeed(true)
    else
      super.isCurrentlyEmpty

  override def wasAlwaysEmpty: Boolean = packed.isEmpty
}

object Node {

  def apply(
      id: NodeId,
      eventsAtTime: Vector[EventsAtTime] = Vector.empty
  ): Node =
    NodeFromEventsAtTime(
      id = id,
      version = eventsAtTime.length,
      latestEventTime = eventsAtTime.lastOption.fold(StartOfTime)(_.eventTime),
      latestSequence = eventsAtTime.lastOption.fold(0)(_.sequence),
      unpackedEventsAtTime = eventsAtTime
    )

  def apply(
      id: NodeId,
      version: Int,
      latest: EventTime,
      sequence: Int,
      packed: PackedNodeContents
  ): Node =
    NodeFromPackedHistory(
      id = id,
      version = version,
      latestEventTime = latest,
      latestSequence = sequence,
      packed = packed
    )
}
