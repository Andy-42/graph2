package andy42.graph.model

import andy42.graph.services.CacheItem
import andy42.graph.model.NodeHistory
import org.msgpack.core.*
import zio.*


type NodeId = Vector[Byte] // Any length, but an 8-byte UUID-like identifier is typical

// The time an event occurs
type EventTime = Long // Epoch Millis
val StartOfTime: EventTime = Long.MinValue
val EndOfTime: EventTime = Long.MaxValue

type Sequence = Int // Break ties when multiple groups of events are processed in the same EventTime

final case class NodeSnapshot(time: EventTime, sequence: Int, properties: PropertySnapshot, edges: EdgeSnapshot)

object NodeSnapshot:
  val empty =
    NodeSnapshot(time = StartOfTime, sequence = 0, properties = PropertySnapshot.empty, edges = EdgeSnapshot.empty)

/**
  * PackedNodeHistory is the packed form of of NodeHistory or GraphHistory.
  * It is not counted like other packed forms so that it can be appended without completely re-writing the entire history.
  * This only works for the outermost packed form since unpacking requires testing that whether there is more packed data to consume.
  */
type PackedNodeHistory = Packed

sealed trait Node:
  def id: NodeId

  def version: Int
  def lastTime: EventTime
  def lastSequence: Int

  def history: IO[UnpackFailure, NodeHistory]
  def packedHistory: PackedNodeHistory

  def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node]

  def current: IO[UnpackFailure, NodeSnapshot]
  def atTime(time: EventTime): IO[UnpackFailure, NodeSnapshot] =
    if time >= lastTime then current
    else
      for nodeHistory <- history
      yield CollapseNodeHistory(nodeHistory, time)

  def hasEmptyHistory: Boolean

final case class NodeImplementation(
    id: NodeId,
    version: Int,
    lastTime: EventTime,
    lastSequence: Int,

    // These may be provided at construction time if they are available
    private val reifiedCurrent: NodeSnapshot | Null = null,
    private val reifiedHistory: NodeHistory | Null = null,
    packedHistory: PackedNodeHistory
) extends Node:

  override val history: IO[UnpackFailure, NodeHistory] =
    if reifiedHistory != null then ZIO.succeed(reifiedHistory)
    else NodeHistory.unpack(using MessagePack.newDefaultUnpacker(packedHistory))

  override def append(events: Vector[Event], time: EventTime): IO[UnpackFailure, Node] =
    require(time >= lastTime)

    val sequence = if time > lastTime then 0 else lastSequence + 1

    for history <- history
    yield copy(
      version = version + 1,
      lastTime = time,
      lastSequence = sequence,
      packedHistory = packedHistory ++ EventsAtTime(time, sequence, events).toPacked
    )

  override val current: IO[UnpackFailure, NodeSnapshot] =
    if (reifiedCurrent != null)
      ZIO.succeed(reifiedCurrent)
    else
      for
        history <- history
        nodeSnapshot = CollapseNodeHistory(history)
      yield nodeSnapshot

  override def hasEmptyHistory: Boolean = packedHistory.isEmpty

object Node:

  // A node with an empty history
  def empty(id: NodeId): Node =
    NodeImplementation(
      id = id,
      version = 0,
      lastTime = StartOfTime,
      lastSequence = 0,
      packedHistory = Array.empty[Byte]
    )

  def replaceWithHistory(
      id: NodeId,
      history: NodeHistory,
      packed: PackedNodeHistory | Null = null,
      current: NodeSnapshot | Null = null
  ): Node =
    require(history.nonEmpty)

    NodeImplementation(
      id = id,
      version = history.length,
      lastTime = history.last.time,
      lastSequence = history.last.sequence,
      reifiedCurrent = if current != null then current else CollapseNodeHistory(history),
      reifiedHistory = history,
      packedHistory = if packed != null then packed else history.toPacked
    )

  def replaceWithPackedHistory(
    id: NodeId,
    packed: PackedNodeHistory,
    history: NodeHistory | Null = null,
    current: NodeSnapshot | Null = null
  ): Node =
    require(packed.nonEmpty)
    NodeImplementation(
      id = id,
      version = history.length,
      lastTime = history.last.time,
      lastSequence = history.last.sequence,
      reifiedCurrent = if current != null then current else CollapseNodeHistory(history),
      reifiedHistory = history,
      packedHistory = if packed != null then packed else history.toPacked
    )

  // A node being created from the cache
  def fromCacheItem(id: NodeId, item: CacheItem): Node =
    NodeImplementation(
      id = id,
      version = item.version,
      lastTime = item.lastTime,
      lastSequence = item.lastSequence,
      reifiedCurrent = item.current, // This may be present in the cache, or null
      packedHistory = item.packed 
    )
