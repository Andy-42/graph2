package andy42.graph.model

import andy42.graph.model.NodeHistory
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import zio._
import andy42.graph.cache.CacheItem

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

  def history: IO[UnpackFailure, NodeHistory]
  def packed: PackedNodeContents

  def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node]

  def current: IO[UnpackFailure, NodeSnapshot]
  def atTime(time: EventTime): IO[UnpackFailure, NodeSnapshot] =
    if time >= lastTime then current
    else
      for nodeHistory <- history
      yield CollapseNodeHistory(nodeHistory, time)

  def wasAlwaysEmpty: Boolean

final case class NodeFromPackedHistory(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,

    // These may be provided if the caller has them available
    private val reifiedCurrent: NodeSnapshot | Null = null,
    private val reifiedHistory: NodeHistory | Null = null,
    packed: PackedNodeContents
) extends Node:

  override val history: IO[UnpackFailure, NodeHistory] =
    if reifiedHistory != null then ZIO.succeed(reifiedHistory)
    else EventHistory.unpack(using MessagePack.newDefaultUnpacker(packed))

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

  override val current: IO[UnpackFailure, NodeSnapshot] =
    if (reifiedCurrent != null)
      ZIO.succeed(reifiedCurrent)
    else
      for
        history <- history
        nodeSnapshot = CollapseNodeHistory(history)
      yield nodeSnapshot

  override def wasAlwaysEmpty: Boolean = packed.isEmpty

object Node:

  // A node with an empty history
  def empty(id: NodeId): Node =
    NodeFromPackedHistory(
      id = id,
      version = 0,
      lastTime = StartOfTime,
      lastSequence = 0,
      packed = Array.empty[Byte]
    )

  def replaceHistory(
      id: NodeId,
      history: NodeHistory,
      packed: PackedNodeContents | Null = null
  ): Node =
    require(history.nonEmpty)

    NodeFromPackedHistory(
      id = id,
      version = history.length,
      lastTime = history.last.time,
      lastSequence = history.last.sequence,
      reifiedCurrent = CollapseNodeHistory(history),
      reifiedHistory = history,
      packed = if packed != null then packed else history.toByteArray
    )

  // A node being created from the cache
  def fromCacheItem(id: NodeId, item: CacheItem): Node =
    NodeFromPackedHistory(
      id = id,
      version = item.version,
      lastTime = item.lastTime,
      lastSequence = item.lastSequence,
      reifiedCurrent = item.current, // This may be present in the cache, or null
      packed = item.packed
    )
