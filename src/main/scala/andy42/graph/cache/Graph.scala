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
 * changes by that mechanism, this API forces changes to be serialize by tracking the node ids that
 * have changes currently in flight in a transactional set.
 */
trait Graph { self => // TODO: Check use of self here

  def get(id: NodeId): ZIO[Clock, UnpackFailure | PersistenceFailure, Node]

  /** Append events to a Node's history. This is the fundamental API that all mutation events are based on.
    */
  def append(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node]

  /** Append events
    */
  def append(
      id: NodeId,
      atTime: EventTime,
      properties: PropertiesAtTime,
      edges: EdgesAtTime
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] = {
    val propertyEvents = properties.map { case (k, v) => PropertyAdded(k, v) }
    val edgeEvents = edges.map(EdgeAdded(_))
    val allEvents = (propertyEvents ++ edgeEvents).toVector
    self.append(id, atTime, allEvents)
  }
}

final case class GraphLive(
    inFlight: TSet[NodeId],
    cache: NodeCache,
    nodeDataService: NodeDataService,
    edgeSynchronization: EdgeSynchronization,
    standingQueryEvaluation: StandingQueryEvaluation
) extends Graph {

  override def get(
      id: NodeId
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodeMutationPermit(id)
        optionNode <- cache.get(id)
        newOrExistingNode <- optionNode.fold {
          // Node does not exist in cache
          nodeDataService.get(id).flatMap { eventsAtTime =>
            if eventsAtTime.isEmpty then
              // Node doesn't have any history in the persisted store, so synthesize an empty node.
              // Since a node with an empty history is always considered to exist, there is no point adding it to the cache.
              ZIO.succeed(Node(id))
            else
              val node = Node(id, eventsAtTime)
              // Create a node from the non-empty history and add it to the cache.
              cache.put(node) *> ZIO.succeed(node)
          }
        } { ZIO.succeed(_) } // Node was fetched from the cache
      yield newOrExistingNode
    }

  override def append(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodeMutationPermit(id)
        existingNode <- get(id)
        newNode <- applyEvents(atTime, events, existingNode)
      yield newNode
    }

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  private def withNodeMutationPermit(id: NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  final case class NodeWithNewEvents(node: Node, optionEventsAtTime: Option[EventsAtTime], mustPersist: Boolean)

  private def applyEvents(
      atTime: EventTime,
      newEvents: Vector[Event],
      node: Node
  ): ZIO[Clock, UnpackFailure | PersistenceFailure, Node] = {

    // Eliminate any duplication within events
    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(newEvents)

    for
      nodeWithNewEvents <-
        if node.wasAlwaysEmpty then
          ZIO.succeed {
            // Creating a new node can be done in an optimized way since we don't need to merge in events.
            val newEventsAtTime = EventsAtTime(eventTime = atTime, sequence = 0, events = deduplicatedEvents)
            NodeWithNewEvents(
              node = Node(node.id, Vector(newEventsAtTime)),
              optionEventsAtTime = Some(newEventsAtTime),
              mustPersist = true
            )
          }
        else
          node.atTime(atTime).flatMap { nodeStateAtTime =>

            // Discard any events that would have no effect on the state at that point in time
            val eventsWithEffect =
              EventHasEffectOps.filterHasEffect(events = deduplicatedEvents, nodeState = nodeStateAtTime)

            if eventsWithEffect.isEmpty then
              ZIO.succeed(NodeWithNewEvents(node = node, optionEventsAtTime = None, mustPersist = false))
            else if atTime >= nodeStateAtTime.eventTime then appendEventsToEndOfHistory(node, eventsWithEffect, atTime)
            else mergeInNewEvents(node = node, newEvents = eventsWithEffect, atTime = atTime)
          }

      // Persist and cache only if there were some changes
      _ <- nodeWithNewEvents.optionEventsAtTime.fold(ZIO.unit) {
        nodeDataService.append(nodeWithNewEvents.node.id, _) *> cache.put(nodeWithNewEvents.node)
      }

      // Handle any events that are a result of this node being appended to.
      // Note that we pass in the events appended, and not the ones that were changed in this operation
      // since we may be re-processing a change and we have to guarantee that the events were processed.
      _ <- standingQueryEvaluation.nodeChanged(nodeWithNewEvents.node, deduplicatedEvents)
      _ <- edgeSynchronization.eventsAppended(nodeWithNewEvents.node.id, atTime, deduplicatedEvents)
    yield nodeWithNewEvents.node
  }

  /** General merge of new events at any point in time into the node's history. This requires unpacking the node's
    * history to a Vector[EventsAtTime] which may require more resources for large nodes.
    */
  private def mergeInNewEvents(
      node: Node,
      newEvents: Vector[Event],
      atTime: EventTime
  ): IO[UnpackFailure, NodeWithNewEvents] =
    for
      originalHistory <- node.eventsAtTime

      (before, after) = originalHistory.partition(_.eventTime <= atTime)

      sequence = before.lastOption match
        case None                                              => 0
        case Some(lastEvents) if lastEvents.eventTime < atTime => 0
        case Some(lastEvents)                                  => lastEvents.sequence + 1

      newEventsAtTime = EventsAtTime(eventTime = atTime, sequence = sequence, events = newEvents)

      newHistory = (before :+ newEventsAtTime) ++ after
    yield NodeWithNewEvents(
      node = Node(node.id, newHistory),
      optionEventsAtTime = Some(newEventsAtTime),
      mustPersist = true
    )

  /** Append events to the end of history. Appending events can be done more efficiently by avoiding the need to unpack
    * all of history to a Vector[EventsAtTime], but instead we can just append the packed history to the end of the
    * packed history. This could be significant for nodes with a large history (esp. may edges).
    */
  private def appendEventsToEndOfHistory(
      node: Node,
      newEvents: Vector[Event],
      atTime: EventTime
  ): IO[UnpackFailure, NodeWithNewEvents] = {
    require(newEvents.nonEmpty)
    require(atTime >= node.latestEventTime)

    val eventsAtTime = EventsAtTime(
      eventTime = atTime,
      sequence = if node.latestEventTime == atTime then node.latestSequence + 1 else 0,
      events = newEvents
    )

    for newNode <- node.append(newEvents, atTime)
    yield NodeWithNewEvents(
      node = newNode,
      optionEventsAtTime = Some(eventsAtTime),
      mustPersist = true
    )
  }
}

object Graph {
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
}
