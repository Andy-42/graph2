package andy42.graph.cache

import andy42.graph.cache.LRUCache
import andy42.graph.model
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

  def get(id: NodeId): IO[UnpackFailure | ReadFailure, Node with PackedNode]

  /** Append events to a Node's history. This is the fundamental API that
    * all mutation events are based on.
    */
  def append(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node]

  /** Append events
    */
  def append(
      id: NodeId,
      atTime: EventTime,
      properties: PropertiesAtTime,
      edges: EdgesAtTime
  ): IO[UnpackFailure | PersistenceFailure, Node] = {
    val propertyEvents = properties.map { case (k, v) => PropertyAdded(k, v) }
    val edgeEvents = edges.map(EdgeAdded(_))
    val allEvents = (propertyEvents ++ edgeEvents).toVector
    self.append(id, atTime, allEvents)
  }
}

case class GraphLive(
    inFlight: TSet[NodeId],
    cache: LRUCache,
    persistor: Persistor,
    edgeSynchronization: EdgeSynchronization,
    standingQueryEvaluation: StandingQueryEvaluation
) extends Graph {

  override def get(
      id: NodeId
  ): IO[UnpackFailure | ReadFailure, Node with PackedNode] =
    ZIO.scoped {
      for {
        _ <- withNodeMutationPermit(id)
        optionNode <- cache.get(id)
        newOrExistingNode <- optionNode.fold {
          // Node does not exist in cache
          persistor.get(id).flatMap { (eventsAtTime: Vector[EventsAtTime]) =>
            if (eventsAtTime.isEmpty)
              // Node doesn't have any history in the persisted store, so synthesize an empty node.
              // Since a node with an empty history is always considered to exist, there is no point adding it to the cache.
              ZIO.succeed(Node(id))
            else {
              val node = Node(id, eventsAtTime)
              // Create a node from the non-empty history and add it to the cache.
              cache.put(node) *> ZIO.succeed(node)
            }
          }
        } { ZIO.succeed(_) } // Node was fetched from the cache
      } yield newOrExistingNode
    }

  override def append(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for {
        _ <- withNodeMutationPermit(id)
        existingNode <- get(id)
        newNode <- applyEvents(atTime, events, existingNode)
      } yield newNode
    }

  private def acquirePermit(id: => NodeId): UIO[NodeId] =
    STM
      .ifSTM(inFlight.contains(id))(STM.retry, inFlight.put(id).map(_ => id))
      .commit

  private def releasePermit(id: => NodeId): UIO[Unit] =
    inFlight.delete(id).commit

  private def withNodeMutationPermit(id: NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

  private def applyEvents(
      atTime: EventTime,
      newEvents: Vector[Event],
      node: Node with PackedNode
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    for {
      // The accumulated node state for all event up to and including atTime
      nodeStateAtTime <- node.atTime(atTime)

      // Eliminate any duplication within events
      deduplicated = EventDeduplicationOps.deduplicateWithinEvents(newEvents)

      // Discard any events that would have no effect on the current state
      eventsWithEffect = EventHasEffectOps.filterHasEffect(
        events = deduplicated,
        nodeState = nodeStateAtTime
      )

      newNode <-
        if (eventsWithEffect.isEmpty)
          ZIO.succeed(node)
        else
          mergeInNewEvents(node, eventsWithEffect, atTime)
    } yield newNode

  private def mergeInNewEvents(
      node: Node with PackedNode,
      newEvents: Vector[Event],
      atTime: EventTime
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    for {
      eventsAtTime <- EventHistory.unpack(node.packed)
      state <- node.atTime(atTime)

      // Remove any duplication within the new events
      eventsWithEffect = EventHasEffectOps.filterHasEffect(newEvents, state)

      newNode <-
        if (eventsWithEffect.isEmpty)
          ZIO.succeed(node)
        else
          appendEventsAtTime(
            id = node.id,
            eventsAtTime,
            atTime,
            newEvents = eventsWithEffect
          ) // TODO: Re-order parameters
    } yield newNode

  private def appendEventsAtTime(
      id: NodeId,
      eventsAtTime: Vector[EventsAtTime],
      atTime: EventTime,
      newEvents: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] = {
    val (before, after) = eventsAtTime.partition(_.eventTime <= atTime)

    val sequence = before.lastOption match {
      case None                                                  => 0
      case Some(eventsAtTime) if eventsAtTime.eventTime < atTime => 0
      case Some(eventsAtTime) => eventsAtTime.sequence + 1
    }

    val newEventsWithEffect =
      EventsAtTime(eventTime = atTime, sequence = sequence, events = newEvents)

    val newEventHistory = (before :+ newEventsWithEffect) ++ after
    val newNode = Node(id, newEventHistory)

    for {
      // Evaluate any standing queries that would affect this node.
      // This is done before it is committed to ensure at-least-once semantics for queries.
      _ <- standingQueryEvaluation.nodeChanged(newNode, newEventsWithEffect)

      // TODO: Commentary on ordering of persistor.append/cache.put
      // The cache can only contain contents that are persisted.
      // Persistor has to do upsert
      // Potential to revise history, but that can be done safely

      _ <- persistor.append(id, newEventsWithEffect)

      _ <- cache.put(newNode)

      // Synchronize any far edges with any edge events
      _ <- edgeSynchronization.nearEdgesSet(id, newEventsWithEffect)

    } yield newNode
  }
}

object Graph {
  val layer: URLayer[LRUCache & Persistor & EdgeSynchronization & StandingQueryEvaluation, Graph] =
    ZLayer {
      for {
        inFlight <- TSet.empty[NodeId].commit
        lruCache <- ZIO.service[LRUCache]
        persistor <- ZIO.service[Persistor]
        edgeSynchronization <- ZIO.service[EdgeSynchronization]
        standingQueryEvaluation <- ZIO.service[StandingQueryEvaluation]
      } yield GraphLive(inFlight, lruCache, persistor, edgeSynchronization, standingQueryEvaluation)
    }
}
