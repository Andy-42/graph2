package andy42.graph.model

import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import zio._

final case class NodeStateAtTime(time: EventTime, sequence: Int, properties: PropertiesAtTime, edges: EdgesAtTime)

type PackedNodeContents = Array[Byte]

sealed trait Node:
  def id: NodeId

  def version: Int
  def lastTime: EventTime
  def lastSequence: Int

  def history: IO[UnpackFailure, NodeHistory]
  def packed: Array[Byte]
  def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node]

  lazy val current: IO[UnpackFailure, NodeStateAtTime] =
    for history <- history
    yield CollapseNodeHistory(history, lastTime)

  def atTime(time: EventTime): IO[UnpackFailure, NodeStateAtTime] =
    if time >= lastTime then current
    else
      for history <- history
      yield CollapseNodeHistory(history, time)

  def wasAlwaysEmpty: Boolean

final case class NodeFromEventsAtTime(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,
    reifiedEventsAtTime: NodeHistory
) extends Node:

  override lazy val packed: Array[Byte] = EventHistory.packToArray(reifiedEventsAtTime)

  override def history: UIO[NodeHistory] = ZIO.succeed(reifiedEventsAtTime)

  override def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node] =
    require(time >= lastTime)

    val sequence = if time > lastTime then 0 else lastSequence + 1

    ZIO.succeed(
      copy(
        version = version + 1,
        lastTime = time,
        lastSequence = sequence,
        reifiedEventsAtTime = reifiedEventsAtTime :+ EventsAtTime(time, sequence, events)
      )
    )

  override def wasAlwaysEmpty: Boolean = reifiedEventsAtTime.isEmpty

final case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,
    packed: PackedNodeContents
) extends Node:

  override lazy val history: IO[UnpackFailure, NodeHistory] =
    EventHistory.unpack(using MessagePack.newDefaultUnpacker(packed))

  override def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node] =
    require(time >= lastTime)

    val sequence = if time > lastTime then 0 else lastSequence + 1

    for history <- history
    yield copy(
      version = version + 1,
      lastTime = time,
      lastSequence = sequence,
      packed = packed ++ EventsAtTime(time, sequence, events).toByteArray
    )

  override def wasAlwaysEmpty: Boolean = packed.isEmpty

object Node:

  def apply(
      id: NodeId,
      history: NodeHistory = Vector.empty
  ): Node =
    NodeFromEventsAtTime(
      id = id,
      version = history.length,
      lastTime = history.lastOption.fold(StartOfTime)(_.time),
      lastSequence = history.lastOption.fold(0)(_.sequence),
      reifiedEventsAtTime = history
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
      lastTime = latest,
      lastSequence = sequence,
      packed = packed
    )
