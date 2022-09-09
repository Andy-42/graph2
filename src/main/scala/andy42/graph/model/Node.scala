package andy42.graph.model

import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.core.MessageBufferPacker
import andy42.graph.model.NodeHistory
import zio._

final case class NodeSnapshot(time: EventTime, sequence: Int, properties: PropertySnapshot, edges: EdgeSnapshot)

object NodeSnapshot:
  val empty =
    NodeSnapshot(time = StartOfTime, sequence = 0, properties = PropertySnapshot.empty, edges = EdgeSnapshot.empty)

type PackedNodeContents = Array[Byte]

sealed trait Node:
  def id: NodeId

  def version: Int
  def lastTime: EventTime
  def lastSequence: Int

  def properties: IO[UnpackFailure, PropertySnapshot]
  def edges: IO[UnpackFailure, EdgeSnapshot]

  def history: IO[UnpackFailure, NodeHistory]
  def packed: PackedNodeContents

  def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node]

  def atTime(time: EventTime): IO[UnpackFailure, NodeSnapshot] =
    if time >= lastTime then
      for
        properties <- properties
        edges <- edges
      yield NodeSnapshot(time = lastTime, sequence = lastSequence, properties = properties, edges = edges)
    else
      for nodeHistory <- history
      yield CollapseNodeHistory(nodeHistory, time)

  def wasAlwaysEmpty: Boolean

final case class NodeFromEventsAtTime(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,
    reifiedNodeHistory: NodeHistory
) extends Node:

  override lazy val packed: PackedNodeContents = EventHistory.packToArray(reifiedNodeHistory)

  val nodeSnapshot: IO[UnpackFailure, NodeSnapshot] =
    ZIO.succeed(CollapseNodeHistory(reifiedNodeHistory)) // TODO: .memoize

  override def properties: IO[UnpackFailure, PropertySnapshot] =
    nodeSnapshot.map(_.properties)

  override def edges: IO[UnpackFailure, EdgeSnapshot] =
    nodeSnapshot.map(_.edges)

  override def history: UIO[NodeHistory] = ZIO.succeed(reifiedNodeHistory)

  override def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node] =
    require(time >= lastTime)

    val sequence = if time > lastTime then 0 else lastSequence + 1

    // TODO: Could move properties forward by one notch rather than forcing the copy to reload from scratch

    ZIO.succeed(
      copy(
        version = version + 1,
        lastTime = time,
        lastSequence = sequence,
        reifiedNodeHistory = reifiedNodeHistory :+ EventsAtTime(time, sequence, events)
      )
    )

  override def wasAlwaysEmpty: Boolean = reifiedNodeHistory.isEmpty

final case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,
    reifiedProperties: PropertySnapshot | Null,
    reifiedEdges: EdgeSnapshot | Null,
    packed: PackedNodeContents
) extends Node:

  val current: IO[UnpackFailure, NodeSnapshot] =
    if (reifiedProperties != null && reifiedEdges != null)
      ZIO.succeed(
        NodeSnapshot(time = lastTime, sequence = lastSequence, properties = reifiedProperties, edges = reifiedEdges)
      )
    else
      for nodeHistory <- history
      yield CollapseNodeHistory(nodeHistory)

  override val edges: IO[UnpackFailure, EdgeSnapshot] =
    if reifiedEdges != null then ZIO.succeed(reifiedEdges)
    else
      for nodeSnapshot <- current
      yield nodeSnapshot.edges

  override val properties: IO[UnpackFailure, PropertySnapshot] =
    if reifiedProperties != null then ZIO.succeed(reifiedProperties)
    else
      for nodeSnapshot <- current
      yield nodeSnapshot.properties

  val memoizedHistory: UIO[IO[UnpackFailure, NodeHistory]] =
    EventHistory.unpack(using MessagePack.newDefaultUnpacker(packed)).memoize

  override val history: IO[UnpackFailure, NodeHistory] =
    for 
      m <- memoizedHistory
      x <- m
    yield x

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

  // A node with an empty history
  def empty(id: NodeId): Node =
    NodeFromPackedHistory(
      id = id,
      version = 0,
      lastTime = StartOfTime,
      lastSequence = 0,
      packed = Array.empty[Byte],
      reifiedProperties = PropertySnapshot.empty,
      reifiedEdges = EdgeSnapshot.empty
    )

  // A node being created from the persistent store
  def apply(
      id: NodeId,
      history: NodeHistory
  ): Node =
    require(history.nonEmpty) // TODO: Is this necessary?

    NodeFromEventsAtTime(
      id = id,
      version = history.length,
      lastTime = history.last.time,
      lastSequence = history.last.sequence,
      reifiedNodeHistory = history
    )

  // A node being created from the cache
  // The properties and/or edges may have been purged from the cache and need to be re-created.
  def apply(
      id: NodeId,
      version: Int,
      lastTime: EventTime,
      lastSequence: Int,
      properties: PropertySnapshot | Null,
      edges: EdgeSnapshot | Null,
      packed: PackedNodeContents
  ): Node =
    NodeFromPackedHistory(
      id = id,
      version = version,
      lastTime = lastTime,
      lastSequence = lastSequence,
      reifiedProperties = properties,
      reifiedEdges = edges,
      packed = packed
    )
