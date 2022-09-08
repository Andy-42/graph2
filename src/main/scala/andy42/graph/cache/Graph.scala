package andy42.graph.cache

import andy42.graph.cache.NodeCache
import andy42.graph.model
import andy42.graph.model.EventDeduplication
import andy42.graph.model.*
import org.msgpack.core.MessagePack
import zio.*
import zio.stm.*

/*
 * This is the entry point to applying any change to a node.
 * While changes can be applied as though they happened at any point in time (and in any order),
 * all changes to a node must be serialized. Rather than using an actor system and serializing the
 * changes by that mechanism, this API forces changes to be serialized by tracking the node ids that
 * have changes currently in flight in a transactional set.
 */
trait Graph:

  def get(id: NodeId): ZIO[Clock, UnpackFailure | PersistenceFailure, Node]

  /** Append events to a Node's history. This is the fundamental API that all mutation events are based on.
    */
  def append(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node]

  /** Append events
    */
  def append(
      id: NodeId,
      time: EventTime,
      properties: PropertiesAtTime = Map.empty,
      edges: EdgesAtTime = Set.empty
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =
    val propertyEvents = properties.map((k, v) => Event.PropertyAdded(k, v))
    val edgeEvents = edges.map(Event.EdgeAdded(_))
    val allEvents = (propertyEvents ++ edgeEvents).toVector

    if allEvents.isEmpty then get(id) else append(id, time, allEvents)

final case class GraphLive(
    inFlight: TSet[NodeId],
    cache: NodeCache,
    nodeDataService: NodeDataService,
    edgeSynchronization: EdgeSynchronization,
    standingQueryEvaluation: StandingQueryEvaluation
) extends Graph:

  override def get(
      id: NodeId
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)

        optionNode <- cache.get(id)
        node <- optionNode.fold {
          // Node does not exist in cache
          nodeDataService.get(id).flatMap { nodeHistory =>
            if nodeHistory.isEmpty then
              // Node doesn't have any history in the persisted store, so synthesize an empty node.
              // Since a node with an empty history is always considered to exist, there is no point adding it to the cache.
              ZIO.succeed(Node(id))
            else
              val node = Node(id, nodeHistory)
              // Create a node from the non-empty history and add it to the cache.
              cache.put(node) *> ZIO.succeed(node)
          }
        } { ZIO.succeed(_) } // Node was fetched from the cache
      yield node
    }

  override def append(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)

        node <- get(id)
        newNode <- applyEvents(time, events, node)
      yield newNode
    }

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  private def withNodePermit(id: NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  private def applyEvents(
      time: EventTime,
      newEvents: Vector[Event],
      node: Node
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =

    // Eliminate any duplication within events
    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(newEvents)

    for
      x <- determineNextNodeStateAndChangesToPersist(node, time, deduplicatedEvents)

      _ <- x.changesToPersist.fold(ZIO.unit) {
        nodeDataService.append(x.nextNodeState.id, _) *> cache.put(x.nextNodeState)
      }

      // Handle any events that are a result of this node being appended to.
      // Note that we notify on deduplicated events and not on the events being persisted.
      // since we may be re-processing a change and we have to guarantee that all post-persist notifications were generated.
      _ <- standingQueryEvaluation.nodeChanged(x.nextNodeState, deduplicatedEvents)
      _ <- edgeSynchronization.eventsAppended(x.nextNodeState.id, time, deduplicatedEvents)
    yield x.nextNodeState

  final case class NextNodeStateAndChangesToPersist(nextNodeState: Node, changesToPersist: Option[EventsAtTime])

  private def determineNextNodeStateAndChangesToPersist(
      node: Node,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure, NextNodeStateAndChangesToPersist] =
    if node.wasAlwaysEmpty then
      ZIO.succeed {
        // Creating a new node can be done in an optimized way since we don't need to merge in events.
        val newEventsAtTime = EventsAtTime(time = time, sequence = 0, events = events)
        NextNodeStateAndChangesToPersist(
          nextNodeState = Node(node.id, Vector(newEventsAtTime)),
          changesToPersist = Some(newEventsAtTime)
        )
      }
    else
      node.atTime(time).flatMap { nodeStateAtTime =>

        // Discard any events that would have no effect on the state at that point in time
        val eventsWithEffect =
          EventHasEffectOps.filterHasEffect(events = events, nodeState = nodeStateAtTime)

        if eventsWithEffect.isEmpty then
          ZIO.succeed(NextNodeStateAndChangesToPersist(nextNodeState = node, changesToPersist = None))
        else if time >= nodeStateAtTime.time then appendEventsToEndOfHistory(node, eventsWithEffect, time)
        else mergeInNewEvents(node = node, newEvents = eventsWithEffect, time = time)
      }

  /** General merge of new events at any point in time into the node's history. This requires unpacking the node's
    * history to a NodeHistory which may require more resources for large nodes.
    */
  private def mergeInNewEvents(
      node: Node,
      newEvents: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, NextNodeStateAndChangesToPersist] =
    for
      originalHistory <- node.history

      (before, after) = originalHistory.partition(_.time <= time)

      sequence = before.lastOption match
        case None                                       => 0
        case Some(lastEvents) if lastEvents.time < time => 0
        case Some(lastEvents)                           => lastEvents.sequence + 1

      newEventsAtTime = EventsAtTime(time = time, sequence = sequence, events = newEvents)

      newHistory = (before :+ newEventsAtTime) ++ after
    yield NextNodeStateAndChangesToPersist(
      nextNodeState = Node(node.id, newHistory),
      changesToPersist = Some(newEventsAtTime)
    )

  /** Append events to the end of history. Appending events can be done more efficiently by avoiding the need to unpack
    * all of history to a NodeHistory, but instead we can just append the packed history to the end of the packed
    * history. This could be significant for nodes with a large history (esp. may edges).
    */
  private def appendEventsToEndOfHistory(
      node: Node,
      events: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, NextNodeStateAndChangesToPersist] =
    require(events.nonEmpty)
    require(time >= node.lastTime)

    val eventsAtTime = EventsAtTime(
      time = time,
      sequence = if node.lastTime == time then node.lastSequence + 1 else 0,
      events = events
    )

    for nextNodeState <- node.append(events, time)
    yield NextNodeStateAndChangesToPersist(
      nextNodeState = nextNodeState,
      changesToPersist = Some(eventsAtTime)
    )

object Graph:
  val layer: URLayer[NodeCache & NodeDataService & EdgeSynchronization & StandingQueryEvaluation, Graph] =
    ZLayer {
      for
        lruCache <- ZIO.service[NodeCache]
        nodeDataService <- ZIO.service[NodeDataService]
        edgeSynchronization <- ZIO.service[EdgeSynchronization]
        standingQueryEvaluation <- ZIO.service[StandingQueryEvaluation]

        inFlight <- TSet.empty[NodeId].commit
      yield GraphLive(inFlight, lruCache, nodeDataService, edgeSynchronization, standingQueryEvaluation)
    }
