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

  /** Append events to the Graph history.
   * On a successful return:
   * - The events will have been appended to the Node history and committed to the persistent store, and
   * - Any standing queries related to these nodes will have been evaluated and any positive match results committed, and
   * - The appending of any far half-edges will have been initiated (but not necessarily committed)
   * 
    *
    * This is the fundamental API that all mutations of the graph are based on.
    * 
    * @param mutations The events to be applied to the graph state.
    * @return The Node state after the events have been applied.
    * The length of this result can be different from `mutations` since multiple
    * mutations can be applied to a single node.
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
      events = changes.collect {
        case input @ GraphMutationInput(id, _) => input.event
      }
    )

  /**
    * TODO: This should be modified to take a batch of changes, and the SQE and edge synch should
    * be done in a separate following step that processes the change for the entire batch.
    * This should be more efficient than doing the batches separately.
    * 
    * The following steps are only guaranteed to be consistent within a batch.
    * TODO: Describe the case where two near-simultaneous transactions could produce 
    *
    * @param id
    * @param time
    * @param events
    * @return
    */
  override def append(
    time: EventTime,
    changes: Vector[GraphMutationInput]
  ): IO[UnpackFailure | PersistenceFailure, Unit] =
    if changes.isEmpty then ZIO.unit
    else 
      for 
        output: Vector[GroupedGraphMutationOutput] <- ZIO.foreachPar(groupChangesByNodeId(changes))(processChangesForOneNode(time, _))
        
        // Handle any events that are a result of this node being appended to.
        // Note that we notify on de-duplicated events and not on the events (if any) that were persisted.
        // This append may be re-processing a change and we have to guarantee that all post-persist notifications were generated.

        // Processing these events are required for the append request to complete successfully,
        // but since the node permit has been released, there is no possibility of deadlock.
        // TODO: Is there potential for deadlock here? Need to make the reasoning explicit.
        _ <- standingQueryEvaluation.graphChanged(time, output)

        // Note that this only queues the edge synchronization and there is no waiting (or possibility of deadlock)
        _ <- edgeSynchronization.graphChanged(time, output)
      yield ()
  
  def processChangesForOneNode(
    time: EventTime, 
    changes: GroupedGraphMutationInput
  ): IO[UnpackFailure | PersistenceFailure, GroupedGraphMutationOutput] = 
    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(changes.events)

    for node <- applyEventsToPersistentStoreAndCache(changes.id, time,deduplicatedEvents)
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
