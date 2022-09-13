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

        optionNode <- cache.get(id)
        node <- optionNode.fold {
          for
            node <- nodeDataService.get(id)
            _ <-
              if node.wasAlwaysEmpty then ZIO.unit
              else cache.put(node)
          yield node
        } { ZIO.succeed } // Node was fetched from the cache
      yield node
    }

  override def append(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)

        existingNode <- get(id)
        newNode <- applyEvents(time, events, existingNode)
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
  ): IO[UnpackFailure | PersistenceFailure, Node] =

    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(newEvents)

    for
      x <- determineNextNodeStateAndHistoryToPersist(node, time, deduplicatedEvents)
      nextNodeState = x._1
      historyToPersist = x._2

      // Persist to the data store and cache, if there is new history to persist
      _ <- historyToPersist.fold(ZIO.unit)(nodeDataService.append(nextNodeState.id, _))
      _ <- historyToPersist.fold(ZIO.unit)(_ => cache.put(nextNodeState))

      // Handle any events that are a result of this node being appended to.
      // Note that we notify on deduplicated events and not on the events being persisted.
      // since we may be re-processing a change and we have to guarantee that all post-persist notifications were generated.
      _ <- standingQueryEvaluation.nodeChanged(nextNodeState, deduplicatedEvents)
      _ <- edgeSynchronization.eventsAppended(nextNodeState.id, time, deduplicatedEvents)
    yield nextNodeState

  private def determineNextNodeStateAndHistoryToPersist(
      node: Node,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure, (Node, Option[EventsAtTime])] =
    if node.wasAlwaysEmpty then
      for
        nextNodeState <- node.append(events, time)
        eventsAtTime = EventsAtTime(time = time, sequence = 0, events = events)
      yield nextNodeState -> Some(eventsAtTime)
    else
      node.atTime(time).flatMap { nodeSnapshot =>

        // Discard any events that would have no effect on the state at that point in time
        val eventsWithEffect = EventHasEffectOps.filterHasEffect(events, nodeSnapshot)

        if eventsWithEffect.isEmpty then ZIO.succeed(node -> None)
        else if time >= nodeSnapshot.time then appendEventsToEndOfHistory(node, eventsWithEffect, time)
        else mergeInNewEvents(node, eventsWithEffect, time)
      }

  /** General merge of new events at any point in time into the node's history. This requires unpacking the node's
    * history to a NodeHistory which may require more resources for large nodes.
    */
  private def mergeInNewEvents(
      node: Node,
      newEvents: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, (Node, Option[EventsAtTime])] =
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
    yield nextNodeState -> Some(newEventsAtTime)

  /** Append events to the end of history. Appending events can be done more efficiently by avoiding the need to unpack
    * all of history to a NodeHistory, but instead we can just append the packed history to the end of the packed
    * history. This could be significant for nodes with a large history (esp. may edges).
    */
  private def appendEventsToEndOfHistory(
      node: Node,
      events: Vector[Event],
      time: EventTime
  ): IO[UnpackFailure, (Node, Option[EventsAtTime])] =
    require(events.nonEmpty)
    require(time >= node.lastTime)

    val eventsAtTime = EventsAtTime(
      time = time,
      sequence = if node.lastTime == time then node.lastSequence + 1 else 0,
      events = events
    )

    for nextNodeState <- node.append(events, time)
    yield nextNodeState -> Some(eventsAtTime)

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
