package andy42.graph.services

import andy42.graph.model.*
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

  def get(id: NodeId): IO[UnpackFailure | PersistenceFailure, Node]

  /** Append events to a Node's history. This is the fundamental API that all mutation events are based on.
    */
  def append(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node]

  /** Append events
    */
  def append(
      id: NodeId,
      time: EventTime,
      properties: PropertySnapshot = Map.empty,
      edges: EdgeSnapshot = Set.empty
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    val propertyEvents = properties.map((k, v) => Event.PropertyAdded(k, v))
    val edgeEvents = edges.map(Event.EdgeAdded(_))
    val allEvents = (propertyEvents ++ edgeEvents).toVector

    if allEvents.isEmpty then get(id) else append(id, time, allEvents)

final case class GraphLive(
    inFlight: TSet[NodeId],
    clock: Clock,
    cache: NodeCache,
    nodeDataService: NodeDataService,
    edgeSynchronization: EdgeSynchronization,
    standingQueryEvaluation: StandingQueryEvaluation
) extends Graph:

  override def get(
      id: NodeId
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        node <- getWithPermitHeld(id)
      yield node
    }

  private def getWithPermitHeld(
      id: NodeId
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    for
      optionNode <- cache.get(id)
      node <- optionNode.fold {
        for
          node <- nodeDataService.get(id)
          _ <- if node.hasEmptyHistory then ZIO.unit else cache.put(node)
        yield node
      } { ZIO.succeed } // Node was fetched from the cache
    yield node

  override def append(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] =

    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(events)

    for
      // Modify the node while holding the permit to change the node state for the shortest time possible
      nextNodeState <- applyEventsToPersistentStoreAndCache(id, time, events)

      // Handle any events that are a result of this node being appended to.
      // Note that we notify on deduplicated events and not on the events (if any) that were persisted.
      // This append may be re-processing a change and we have to guarantee that all post-persist notifications were generated.

      // Processing these events are required for the append request to complete successfully,
      // but since the node permit has been released, there is no possiblity of deadlock.
      _ <- standingQueryEvaluation.nodeChanged(nextNodeState, deduplicatedEvents)
      _ <- edgeSynchronization.eventsAppended(nextNodeState.id, time, deduplicatedEvents)
    yield nextNodeState

  private def applyEventsToPersistentStoreAndCache(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        existingNode <- getWithPermitHeld(id)

        x <- determineNextNodeStateAndHistoryToPersist(existingNode, time, events)
        nextNode = x.nextNode
        changes: Option[EventsAtTime] = x.changes

        // Persist to the data store and cache, if there is new history to persist
        _ <- changes.fold(ZIO.unit)(nodeDataService.append(nextNode.id, _))
        _ <- changes.fold(ZIO.unit)(_ => cache.put(nextNode))
      yield nextNode
    }

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  /** Only one fiber can modify the state of a Node (either through get or append) at one time. Rather than each node
    * serializing its mutations (e.g., through an Actor mailbox), this graph serializes access by
    * 
    * If the node permit is currently being held by another fiber, acquiring the permit will
    * wait (on an STM retry) until it is released.
    */
  private def withNodePermit(id: NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  private def determineNextNodeStateAndHistoryToPersist(
      node: Node,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure, NextNodeAndEvents] =
    if node.hasEmptyHistory then
      for
        nextNodeState <- node.append(events, time)
        eventsAtTime = EventsAtTime(time = time, sequence = 0, events = events)
      yield NextNodeAndEvents(nextNodeState, Some(eventsAtTime))
    else
      node.atTime(time).flatMap { nodeSnapshot =>

        // Discard any events that would have no effect on the state at that point in time
        val eventsWithEffect = EventHasEffectOps.filterHasEffect(events, nodeSnapshot)

        if eventsWithEffect.isEmpty then ZIO.succeed(NextNodeAndEvents(node, changes = None))
        else if time >= nodeSnapshot.time then appendEventsToEndOfHistory(node, eventsWithEffect, time)
        else mergeInNewEvents(node, eventsWithEffect, time)
      }

  /** General merge of new events at any point in time into the node's history. This requires unpacking the node's history
    * to a NodeHistory which may require more resources for large nodes.
    */
  private def mergeInNewEvents(
      node: Node,
      newEvents: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, NextNodeAndEvents] =
    for
      originalHistory <- node.history

      (before, after) = originalHistory.partition(_.time <= time)

      sequence = before.lastOption match
        case None                                     => 0
        case Some(lastEvent) if lastEvent.time < time => 0
        case Some(lastEvent)                          => lastEvent.sequence + 1

      newEventsAtTime = EventsAtTime(time = time, sequence = sequence, events = newEvents)
      newHistory = (before :+ newEventsAtTime) ++ after
      nextNodeState = Node.replaceWithHistory(node.id, newHistory)
    yield NextNodeAndEvents(nextNodeState, Some(newEventsAtTime))

  /** Append events to the end of history. Appending events can be done more efficiently by avoiding the need to unpack
    * all of history to a NodeHistory, but instead we can just append the packed history to the end of the packed history.
    * This could be significant for nodes with a large history (esp. may edges).
    */
  private def appendEventsToEndOfHistory(
      node: Node,
      events: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, NextNodeAndEvents] =
    require(events.nonEmpty)
    require(time >= node.lastTime)

    val eventsAtTime = EventsAtTime(
      time = time,
      sequence = if node.lastTime == time then node.lastSequence + 1 else 0,
      events = events
    )

    for nextNodeState <- node.append(events, time)
    yield NextNodeAndEvents(nextNodeState, Some(eventsAtTime))

  case class NextNodeAndEvents(nextNode: Node, changes: Option[EventsAtTime])

end GraphLive

object Graph:
  val layer: URLayer[Clock & NodeCache & NodeDataService & EdgeSynchronization & StandingQueryEvaluation, Graph] =
    ZLayer {
      for
        clock <- ZIO.service[Clock]
        nodeCache <- ZIO.service[NodeCache]
        nodeDataService <- ZIO.service[NodeDataService]
        edgeSynchronization <- ZIO.service[EdgeSynchronization]
        standingQueryEvaluation <- ZIO.service[StandingQueryEvaluation]

        inFlight <- TSet.empty[NodeId].commit
      yield GraphLive(inFlight, clock, nodeCache, nodeDataService, edgeSynchronization, standingQueryEvaluation)
    }
