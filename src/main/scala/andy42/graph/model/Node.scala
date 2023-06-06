package andy42.graph.model

import andy42.graph.model.NodeHistory
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.CacheItem
import org.msgpack.core.*
import zio.*

import scala.util.hashing.MurmurHash3

/** The time an event occurs */
type EventTime = Long // Epoch Millis
val StartOfTime: EventTime = Long.MinValue
val EndOfTime: EventTime = Long.MaxValue

/** Break ties when multiple groups of events are processed for the same EventTime 
  * The ordering of the EventsAtTime corresponds to the time that the events are appended.
  */
type Sequence = Int

final case class NodeSnapshot(time: EventTime, sequence: Int, properties: PropertySnapshot, edges: EdgeSnapshot)

object NodeSnapshot:
  val empty: NodeSnapshot =
    NodeSnapshot(time = StartOfTime, sequence = 0, properties = PropertySnapshot.empty, edges = EdgeSnapshot.empty)

/** PackedNodeHistory is the packed form of of NodeHistory or GraphHistory. It is not counted like other packed forms so
  * that it can be appended without completely re-writing the entire history. This only works for the outermost packed
  * form since unpacking requires testing that whether there is more packed data to consume.
  */
type PackedNodeHistory = Packed

type NodeIO[T] = IO[UnpackFailure | PersistenceFailure, T]

sealed trait Node:
  val id: NodeId

  val version: Int
  val lastTime: EventTime
  val lastSequence: Int

  def history: IO[UnpackFailure, NodeHistory]
  def packedHistory: PackedNodeHistory

  def current: IO[UnpackFailure, NodeSnapshot]

  def atTime(time: EventTime): IO[UnpackFailure, NodeSnapshot] =
    if time >= lastTime then current
    else
      for nodeHistory <- history
      yield CollapseNodeHistory(nodeHistory, time)

  def hasEmptyHistory: Boolean

  def append(time: EventTime, events: Vector[Event]): IO[UnpackFailure, Node] =
    for
      x <- appendWithEventsAtTime(time, events)
      (node, _) = x
    yield node

  def appendWithEventsAtTime(time: EventTime, events: Vector[Event]): IO[UnpackFailure, (Node, Option[EventsAtTime])]

final class NodeImplementation(
    val id: NodeId,
    val version: Int,
    val lastTime: EventTime,
    val lastSequence: Int,

    // These may be provided at construction time if they are available
    private val reifiedCurrent: NodeSnapshot | Null = null,
    private val reifiedHistory: NodeHistory | Null = null,
    val packedHistory: PackedNodeHistory
) extends Node:

  override val history: IO[UnpackFailure, NodeHistory] =
    if reifiedHistory != null then ZIO.succeed(reifiedHistory)
    else NodeHistory.unpack(using MessagePack.newDefaultUnpacker(packedHistory))

  override val current: IO[UnpackFailure, NodeSnapshot] =
    if (reifiedCurrent != null)
      ZIO.succeed(reifiedCurrent)
    else
      for
        history <- history
        nodeSnapshot = CollapseNodeHistory(history)
      yield nodeSnapshot

  override def hasEmptyHistory: Boolean = packedHistory.isEmpty

  override def appendWithEventsAtTime(
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure, (Node, Option[EventsAtTime])] =
    require(events.nonEmpty)

    if hasEmptyHistory then
      val eventsAtTime = EventsAtTime(time = time, sequence = 0, events = events)
      val nextNodeState = Node.fromHistory(id, Vector(eventsAtTime))
      ZIO.succeed(nextNodeState -> Some(eventsAtTime))
    else
      for
        history <- history

        snapshot =
          if time >= lastTime && reifiedCurrent != null then reifiedCurrent
          else CollapseNodeHistory(history, time)
      yield
        // Discard any events that would have no effect on the state at that point in time
        val eventsWithEffect = EventHasEffectOps.filterHasEffect(events, snapshot)

        if eventsWithEffect.isEmpty then this -> None
        else if time >= lastTime then appendEventsToEndOfHistory(time, history, snapshot, eventsWithEffect)
        else appendEventsWithinHistory(time, history, eventsWithEffect)

  private def appendEventsWithinHistory(
      time: EventTime,
      originalHistory: NodeHistory,
      newEvents: Vector[Event]
  ): (Node, Option[EventsAtTime]) =
    require(time < lastTime)

    val (before, after) = originalHistory.partition(_.time <= time)

    val sequence = before.lastOption match
      case None                                     => 0
      case Some(lastEvent) if lastEvent.time < time => 0
      case Some(lastEvent)                          => lastEvent.sequence + 1

    val newEventsAtTime = EventsAtTime(time = time, sequence = sequence, events = newEvents)
    val newHistory = (before :+ newEventsAtTime) ++ after
    val nextNodeState = Node.fromHistory(id, newHistory)

    nextNodeState -> Some(newEventsAtTime)

  private def appendEventsToEndOfHistory(
      time: EventTime,
      history: NodeHistory,
      currentSnapshot: NodeSnapshot,
      events: Vector[Event]
  ): (Node, Option[EventsAtTime]) =
    require(time >= lastTime)

    val eventsAtTime = EventsAtTime(
      time = time,
      sequence = if lastTime == time then lastSequence + 1 else 0,
      events = events
    )

    // Appending to the end of history allows the next current NodeSnapshot to
    // be generated without collapsing all of history.
    val nextCurrent = CollapseNodeHistory(
      history = Vector(eventsAtTime),
      previousSnapshot = Some(currentSnapshot),
      time = time
    )

    val nextNodeState = Node.fromHistory(id, history :+ eventsAtTime, current = nextCurrent)

    nextNodeState -> Some(eventsAtTime)

  // version, lastTime and lastSequence are redundant wrt the hash code
  override def hashCode: Int =
    id.hashCode * 41 + MurmurHash3.arrayHash(packedHistory)

  override def equals(other: Any): Boolean =
    if !other.isInstanceOf[NodeImplementation] then false
    else
      val otherNode = other.asInstanceOf[NodeImplementation]

      id == otherNode.id &&
      packedHistory.sameElements(otherNode.packedHistory)

  override def toString: String =
    s"Node(id=$id, packedHistory.hash=${MurmurHash3.arrayHash(packedHistory)}, version=$version, lastTime=$lastTime)"

object Node:

  // A node with an empty history
  def empty(id: NodeId): Node =
    new NodeImplementation(
      id = id,
      version = 0,
      lastTime = StartOfTime,
      lastSequence = 0,
      packedHistory = Array.empty[Byte]
    )

  def fromHistory(
      id: NodeId,
      history: NodeHistory,
      packed: PackedNodeHistory | Null = null,
      current: NodeSnapshot | Null = null
  ): Node =
    if history.isEmpty then Node.empty(id)
    else
      new NodeImplementation(
        id = id,
        version = history.length,
        lastTime = history.last.time,
        lastSequence = history.last.sequence,
        reifiedCurrent = if current != null then current else CollapseNodeHistory(history),
        reifiedHistory = history,
        packedHistory = if packed != null then packed else history.toPacked
      )

  def fromPackedHistory(
      id: NodeId,
      packed: PackedNodeHistory,
      history: NodeHistory | Null = null,
      current: NodeSnapshot | Null = null
  ): Node =
    require(packed.nonEmpty)

    new NodeImplementation(
      id = id,
      version = history.length,
      lastTime = history.last.time,
      lastSequence = history.last.sequence,
      reifiedCurrent = if current != null then current else CollapseNodeHistory(history),
      reifiedHistory = history,
      packedHistory = packed
    )

  // A node being created from the cache
  def fromCacheItem(id: NodeId, item: CacheItem): Node =
    new NodeImplementation(
      id = id,
      version = item.version,
      lastTime = item.lastTime,
      lastSequence = item.lastSequence,
      reifiedCurrent = item.current, // This may be present in the cache, or null
      packedHistory = item.packed
    )
