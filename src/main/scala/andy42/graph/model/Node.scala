package andy42.graph.model

import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import zio._

finalcase class NodeStateAtTime(eventTime: EventTime, sequence: Int, properties: PropertiesAtTime, edges: EdgesAtTime)

type PackedNodeContents = Array[Byte]

sealed trait Node {
  def id: NodeId

  def version: Int
  def latestEventTime: EventTime
  def latestSequence: Int

  def eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]]
  def packed: Array[Byte]
  def append(events: Vector[Event], atTime: EventTime): IO[UnpackFailure, Node]

  lazy val current: IO[UnpackFailure, NodeStateAtTime] =
    for eventsAtTime <- eventsAtTime
    yield CollapseNodeHistory(eventsAtTime, latestEventTime)

  def atTime(atTime: EventTime): IO[UnpackFailure, NodeStateAtTime] =
    if atTime >= latestEventTime then current
    else
      for eventsAtTime <- eventsAtTime
      yield CollapseNodeHistory(eventsAtTime, atTime)

  def wasAlwaysEmpty: Boolean
}

final case class NodeFromEventsAtTime(
    id: NodeId,
    version: Int,
    latestEventTime: EventTime,
    latestSequence: Int,
    reifiedEventsAtTime: Vector[EventsAtTime]
) extends Node {

  override lazy val packed: Array[Byte] = EventHistory.packToArray(reifiedEventsAtTime)

  override def eventsAtTime: UIO[Vector[EventsAtTime]] = ZIO.succeed(reifiedEventsAtTime)

  override def append(events: Vector[Event], atTime: EventTime): IO[UnpackFailure, Node] = {
    require(atTime >= latestEventTime)

    val sequence = if atTime > latestEventTime then 0 else latestSequence + 1

    ZIO.succeed(
      copy(
        version = version + 1,
        latestEventTime = atTime,
        latestSequence = sequence,
        reifiedEventsAtTime = reifiedEventsAtTime :+ EventsAtTime(atTime, sequence, events)
      )
    )
  }

  override def wasAlwaysEmpty: Boolean = reifiedEventsAtTime.isEmpty
}

final case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    latestEventTime: EventTime,
    latestSequence: Int,
    packed: PackedNodeContents
) extends Node {

  override lazy val eventsAtTime: IO[UnpackFailure, Vector[EventsAtTime]] =
    EventHistory.unpack(MessagePack.newDefaultUnpacker(packed))

  override def append(events: Vector[Event], atTime: EventTime): IO[UnpackFailure, Node] = {
    require(atTime >= latestEventTime)

    val sequence = if atTime > latestEventTime then 0 else latestSequence + 1

    for eventsAtTime <- eventsAtTime
    yield copy(
      version = version + 1,
      latestEventTime = atTime,
      latestSequence = sequence,
      packed = packed ++ EventsAtTime(atTime, sequence, events).toByteArray
    )
  }

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
      reifiedEventsAtTime = eventsAtTime
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
