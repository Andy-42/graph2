package andy42.graph.services

import andy42.graph.matcher.{Matcher, SubgraphSpec}
import andy42.graph.model.*
import zio.*
import zio.stm.*
import zio.telemetry.opentelemetry.tracing.Tracing

/** The Graph service represents the entire state of a graph.
  *
  * The Graph API consists of only two fundamental APIs:
  *   - `get` gets the Node given its NodeId.
  *   - `append` appends events to the Node's state at some point in time.
  *
  * All operations can affect the state of the Node in the NodeCache, including `get`, which can modify the last access
  * time of the cached node. `append` operations may require reading from the cache (in one STM transaction),
  * reading/writing to the persistent store (and effectful operation), followed by writing to the cache (a second STM
  * transaction). This means that coherency of the cache cannot be maintained within a single transaction, this service
  * implements a mechanism that serializes all operations on a Node. The graph is free to process operations on a Node
  * in any order.
  *
  * While changes can be applied as though they happened at any point in time (and in any order), all changes to a node
  * must be serialized. Rather than using an actor system and serializing the changes by that mechanism, this API forces
  * changes to be serialized by tracking the node ids that have changes currently in flight in a transactional set.
  *
  * In addition to managing the state of the Nodes in the cache and persistent store, the Graph implementation also
  * notifies downstream services of changes to the graph:
  *   - Edge Synchronization propagates edge events on the near node to the other node.
  *   - Standing Query Evaluation matches any node changes to standing queries.
  *
  * Since all changes to a node are serialized, attempting to propagate edge events to the far nodes within the
  * transaction would create the possibility of a communications deadlock. For that reason, edge reconciliation is done
  * in an eventually consistent way.
  *
  * Edge Synchronization and Standing Query Evaluation notifications (as well as `append`) use at-least-once semantics.
  * For example, if the graph is consuming events from a Kafka stream, a failed window can be re-processed by the Graph
  * safely.
  */
trait Graph:

  /** Get a node from the graph.
    *
    * The returned node will contain the full history of the node.
    */
  def get(id: NodeId): NodeIO[Node]

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
    * All the processing done is lock free, and there is no possibility of deadlock.
    *
    * @param time
    *   The time that the mutations occurred.
    * @param mutations
    *   The events to be applied to the graph state.
    */
  def append(
      time: EventTime,
      mutations: Vector[NodeMutationInput]
  ): NodeIO[Unit]

  def registerStandingQuery(subgraphSpec: SubgraphSpec): UIO[Unit]

  def appendFarEdgeEvents(
      time: EventTime,
      mutation: NodeMutationInput
  ): NodeIO[Unit]

final case class NodeMutationInput(id: NodeId, events: Vector[Event])

final case class NodeMutationOutput(node: Node, events: Vector[Event])

final case class GraphLive(
    inFlight: TSet[NodeId],
    cache: NodeCache,
    nodeRepositoryService: NodeRepository,
    edgeSynchronization: EdgeSynchronization,
    matchSink: MatchSink,
    tracing: Tracing,
    standingQueries: Ref[Set[SubgraphSpec]]
) extends Graph:

  override def get(
      id: NodeId
  ): NodeIO[Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        node <- getWithPermitHeld(id)
      yield node
    }

  private def getWithPermitHeld(id: NodeId): NodeIO[Node] =
    for
      optionNode <- cache.get(id)
      node <- optionNode.fold(getNodeFromDataServiceAndAddToCache(id))(ZIO.succeed)
    yield node

  private def getWithPermitHeldNoCache(id: NodeId): NodeIO[Node] =
    for
      optionNode <- cache.get(id)
      node <- optionNode.fold(nodeRepositoryService.get(id))(ZIO.succeed)
    yield node

  private def getNodeFromDataServiceAndAddToCache(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    for
      node <- nodeRepositoryService.get(id)
      _ <- cache.put(node).unless(node.hasEmptyHistory)
    yield node

  override def append(
      time: EventTime,
      changes: Vector[NodeMutationInput]
  ): NodeIO[Unit] =
    require(changes.forall(_.events.forall(!_.isFarEdgeEvent)))

    for
      output <- ZIO.foreachPar(changes)(processChangesForOneNode(time, _))

      // TODO: Should fork here since the design edge reconciliation is explicitly eventually consistent.
      // This complicates testing so there should be a way to configure this to fork or not.
      _ <- edgeSynchronization.graphChanged(time, output) // .fork

      // Expand the changed nodes to include the targets of any affected nodes
      x <- allAffectedNodes(time, output)
      subgraphSpecs <- standingQueries.get
      _ <- ZIO.foreachParDiscard(subgraphSpecs)(matchAgainstStandingQueries(time, x))
    yield ()

  // TODO: Can remove this and if there is a need to bypass SQE for far edge creation, that can be detected and handled (use config)
  override def appendFarEdgeEvents(
      time: EventTime,
      mutation: NodeMutationInput
  ): NodeIO[Unit] =
    require(mutation.events.forall(_.isFarEdgeEvent))

    for
      output <- processChangesForOneNode(time, mutation)
      // SQE is not run since those events are processed by SQE in the original append
      // TODO: This is a questionable decision - since it is possible that this could miss matches
      _ <- edgeSynchronization.graphChanged(time, Vector(output)) // add far edge events into reconciliation
    yield ()

  private def processChangesForOneNode(
      time: EventTime,
      changes: NodeMutationInput
  ): NodeIO[NodeMutationOutput] =
    val deduplicatedEvents = EventDeduplication.deduplicateWithinEvents(changes.events)

    for node <- applyEventsToPersistentStoreAndCache(changes.id, time, deduplicatedEvents)
    yield NodeMutationOutput(node, deduplicatedEvents)

  private def applyEventsToPersistentStoreAndCache(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): NodeIO[Node] =
    ZIO.scoped {
      for
        _ <- withNodePermit(id)
        existingNode <- getWithPermitHeldNoCache(id) // Do not cache on retrieval since we will cache shortly

        tuple <- existingNode.appendWithEventsAtTime(time, events)
        (nextNode, maybeEventsAtTime) = tuple

        // Persist to the data store and cache, if there is new history to persist
        _ <- maybeEventsAtTime.fold(ZIO.unit)(nodeRepositoryService.append(nextNode.id, _))
        _ <- cache.put(nextNode).unless(maybeEventsAtTime.isEmpty)
      yield nextNode
    }

  override def registerStandingQuery(subgraphSpec: SubgraphSpec): UIO[Unit] = standingQueries.update(_ + subgraphSpec)

  /** Get the new state for all nodes that were modified in this group of changes. The Graph will pass the new node
    * states after the changes have been applied, but since it will not include any far edge events (since that is done
    * in an eventually-consistent way), so we retrieve any nodes affected by edge events and patch them as though the
    * far edges had been modified.
    */
  private def allAffectedNodes(
      time: EventTime,
      mutations: Vector[NodeMutationOutput]
  ): NodeIO[Vector[Node]] =

    def farEdgeEvents: Vector[(NodeId, Event)] =
      for
        mutation <- mutations
        NodeMutationOutput(node, events) = mutation
        referencedNodeAndFarEdgeEvent <- events.collect {
          case event: Event.EdgeAdded   => event.edge.other -> Event.FarEdgeAdded(event.edge.reverse(node.id))
          case event: Event.EdgeRemoved => event.edge.other -> Event.FarEdgeRemoved(event.edge.reverse(node.id))
        }
      yield referencedNodeAndFarEdgeEvent

    val farEdgeEventsGroupedById: Map[NodeId, Vector[Event]] =
      farEdgeEvents
        .groupBy((id, _) => id)
        .map((k, v) => k -> v.map(_._2))

    // Nodes referenced in NearEdge Events that are not already part of this mutation
    val additionalReferencedNodeIds = farEdgeEventsGroupedById.keys.filter { id =>
      !mutations.exists(_.node.id == id)
    }

    def appendFarEdgeEvents(node: Node): NodeIO[Node] =
      farEdgeEventsGroupedById
        .get(node.id)
        .fold(ZIO.succeed(node))(events => node.append(time, events))

    for
      additionalReferencedNodes <- ZIO.foreachPar(additionalReferencedNodeIds)(get)
      updatedNodes <- ZIO.foreach(mutations.map(_.node) ++ additionalReferencedNodes)(appendFarEdgeEvents)
    yield updatedNodes

  private def matchAgainstStandingQueries(
      time: EventTime,
      changedNodes: Vector[Node]
  )(subgraphSpec: SubgraphSpec): NodeIO[Unit] =
    for
      matcher <- Matcher.make(
        time = time,
        graph = this,
        tracing = tracing,
        subgraphSpec = subgraphSpec,
        nodes = changedNodes
      ) // TODO: Fix order of parameters
      nodeMatches <- matcher.matchNodes(changedNodes.map(_.id))
      _ <- matchSink.offer(SubgraphMatchAtTime(time, subgraphSpec, nodeMatches)).when(nodeMatches.nonEmpty)
    yield ()

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
  val layer: URLayer[NodeCache & NodeRepository & EdgeSynchronization & MatchSink & Tracing, Graph] =
    ZLayer {
      for
        nodeCache <- ZIO.service[NodeCache]
        nodeDataService <- ZIO.service[NodeRepository]
        edgeSynchronization <- ZIO.service[EdgeSynchronization]
        matchSink <- ZIO.service[MatchSink]
        tracing <- ZIO.service[Tracing]

        standingQueries <- Ref.make(Set.empty[SubgraphSpec])
        inFlight <- TSet.empty[NodeId].commit
      yield GraphLive(
        inFlight = inFlight,
        cache = nodeCache,
        nodeRepositoryService = nodeDataService,
        edgeSynchronization = edgeSynchronization,
        matchSink = matchSink,
        tracing = tracing,
        standingQueries = standingQueries
      )
    }
