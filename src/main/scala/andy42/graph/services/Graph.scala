package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.stm.*

/*
 * The Graph service represents the entire state of a graph.
 *
 * The Graph API consists of only two fundamental APIs:
 *  - `get` gets the Node given its NodeId.
 *  - `append` appends events to the Node's state at some point in time.
 *
 * All operations can affect the state of the Node in the NodeCache, including `get`, which
 * can modify the last access time of the cached node. `append` operations may require
 * reading from the cache (in one STM transaction), reading/writing to the persistent store
 * (and effectful operation), followed by writing to the cache (a second STM transaction).
 * This means that coherency of the cache cannot be maintained within a single transaction,
 * this service implements a mechanism that serializes all operations on a Node.
 * The graph is free to process operations on a Node in any order.
 *
 * While changes can be applied as though they happened at any point in time (and in any order),
 * all changes to a node must be serialized. Rather than using an actor system and serializing the
 * changes by that mechanism, this API forces changes to be serialized by tracking the node ids that
 * have changes currently in flight in a transactional set.
 *
 * In addition to managing the state of the Nodes in the cache and persistent store, the
 * Graph implementation also notifies downstream services of changes to the graph:
 *  - Edge Synchronization propagates edge events on the near node to the other node.
 *  - Standing Query Evaluation matches any node changes to standing queries.
 *
 * Since all changes to a node are serialized,
 * attempting to propagate edge events to the far nodes within the transaction would
 * create the possibility of a communications deadlock. For that reason, edge reconciliation
 * is done in an eventually consistent way.
 *
 * Edge Synchronization and Standing Query Evaluation notifications (as well as `append`) use
 * at-least-once semantics. For example, if the graph is consuming events from a Kafka stream,
 * a failed window can be re-processed by the Graph safely.
 */
trait Graph:

  /** Get a node from the graph.
    *
    * The returned node will contain the full history of the node.
    */
  def get(id: NodeId): IO[UnpackFailure | PersistenceFailure, Node]

  /** Append events to the Graph history. On a successful return:
    *   - The events will have been appended to the Node history and committed to the persistent store, and
    *   - Any standing queries related to these nodes will have been evaluated and any positive match results committed,
    *     and
    *   - The appending and reconciliation accounting of any far half-edges will have been initiated (but not
    *     necessarily committed)
    *
    * This is the fundamental API that all mutations of the graph are based on.
    *
    * The processing is done in an idempotent way. This may result in duplicate standing query hits in the case that a
    * failed `append` is retried.
    *
    * Far edge creation and edge reconciliation is necessarily done in an eventually consistent way. If this were done
    * in the scope of this method, then it could create the potential for deadlock. The edge reconciliation scheme
    * provides an auditing mechanism to ensure that all edges occur in balanced pairs.
    *
    * All the processing done is lock free, and there is no possiblity of deadlock.
    *
    * @param time
    *   The time that the mutations occurred.
    * @param mutations
    *   The events to be applied to the graph state.
    */
  def append(time: EventTime, mutations: Vector[GraphMutationInput]): IO[UnpackFailure | PersistenceFailure, Unit]

final case class GraphMutationInput(
    id: NodeId,
    event: Event
)

final case class GroupedGraphMutationInput(
    id: NodeId,
    events: Vector[Event]
)

final case class GroupedGraphMutationOutput(
    node: Node,
    events: Vector[Event]
)

final case class GraphLive(
    inFlight: TSet[NodeId],
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

  private def getWithPermitHeld(id: NodeId): IO[UnpackFailure | PersistenceFailure, Node] =
    for
      optionNode <- cache.get(id)
      node <- optionNode.fold(getNodeFromDataServiceAndAddToCache(id))(ZIO.succeed)
    yield node

  private def getNodeFromDataServiceAndAddToCache(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    for
      node <- nodeDataService.get(id)
      _ <- if node.hasEmptyHistory then ZIO.unit else cache.put(node)
    yield node

  def groupChangesByNodeId(changes: Vector[GraphMutationInput]): Vector[GroupedGraphMutationInput] =
    for id <- changes.map(_.id).distinct
    yield GroupedGraphMutationInput(
      id = id,
      events = changes.collect { case input @ GraphMutationInput(id, _) =>
        input.event
      }
    )

  override def append(
      time: EventTime,
      changes: Vector[GraphMutationInput]
  ): IO[UnpackFailure | PersistenceFailure, Unit] =
    if changes.isEmpty then ZIO.unit
    else
      for
        output <- ZIO.foreachPar(groupChangesByNodeId(changes))(processChangesForOneNode(time, _))
        _ <- standingQueryEvaluation.graphChanged(time, output)
        _ <- edgeSynchronization.graphChanged(time, output)
      yield ()

  def processChangesForOneNode(
      time: EventTime,
      changes: GroupedGraphMutationInput
  ): IO[UnpackFailure | PersistenceFailure, GroupedGraphMutationOutput] =
    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(changes.events)

    for node <- applyEventsToPersistentStoreAndCache(changes.id, time, deduplicatedEvents)
    yield GroupedGraphMutationOutput(node, deduplicatedEvents)

  private def applyEventsToPersistentStoreAndCache(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): IO[UnpackFailure | PersistenceFailure, Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        existingNode <- getWithPermitHeld(id)

        tuple <- existingNode.appendWithEventsAtTime(time, events)
        (nextNode, maybeEventsAtTime) = tuple

        // Persist to the data store and cache, if there is new history to persist
        _ <- maybeEventsAtTime.fold(ZIO.unit)(nodeDataService.append(nextNode.id, _))
        _ <- maybeEventsAtTime.fold(ZIO.unit)(_ => cache.put(nextNode))
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
    * If the node permit is currently being held by another fiber, acquiring the permit will wait (on an STM retry)
    * until it is released.
    */
  private def withNodePermit(id: => NodeId): ZIO[Scope, Nothing, NodeId] =
    ZIO.acquireRelease(acquirePermit(id))(releasePermit(_))

object Graph:
  val layer: URLayer[NodeCache & NodeDataService & EdgeSynchronization & StandingQueryEvaluation, Graph] =
    ZLayer {
      for
        nodeCache <- ZIO.service[NodeCache]
        nodeDataService <- ZIO.service[NodeDataService]
        edgeSynchronization <- ZIO.service[EdgeSynchronization]
        standingQueryEvaluation <- ZIO.service[StandingQueryEvaluation]

        inFlight <- TSet.empty[NodeId].commit
      yield GraphLive(inFlight, nodeCache, nodeDataService, edgeSynchronization, standingQueryEvaluation)
    }
