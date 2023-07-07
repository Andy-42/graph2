package andy42.graph.services

import andy42.graph.config.{AppConfig, MatcherConfig}
import andy42.graph.matcher.{Matcher, SubgraphSpec}
import andy42.graph.model.*
import andy42.graph.persistence.{NodeRepository, PersistenceFailure}
import zio.*
import zio.stm.*
import zio.telemetry.opentelemetry.context.ContextStorage
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

  def start: UIO[Unit]
  def stop: UIO[Unit]

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
    * @param changes
    *   The events to be applied to the graph state.
    */
  def append(
      time: EventTime,
      changes: Vector[NodeMutationInput]
  ): NodeIO[Unit]

  def registerStandingQuery(subgraphSpec: SubgraphSpec): UIO[Unit]

final case class NodeMutationInput(id: NodeId, events: Vector[Event])

extension (changes: Vector[NodeMutationInput])
  def hasEventsOtherThanFarEdge: Boolean = changes.exists(_.events.exists(!_.isFarEdgeEvent))

final case class NodeMutationOutput(node: Node, events: Vector[Event])

final case class GraphLive(
    config: AppConfig,
    inFlight: TSet[NodeId],
    cache: NodeCache,
    nodeRepositoryService: NodeRepository,
    edgeSynchronizationFactory: EdgeSynchronizationFactory,
    edgeSynchronizationRef: Ref[Option[EdgeSynchronization]],
    matchSink: MatchSink,
    tracing: Tracing,
    standingQueries: Ref[Set[SubgraphSpec]],
    contextStorage: ContextStorage
) extends Graph:

  override def start: UIO[Unit] =
    for
      edgeSynchronizationInstance <- edgeSynchronizationFactory.make(this)
      _ <- edgeSynchronizationInstance.startReconciliation
      _ <- edgeSynchronizationRef.set(Some(edgeSynchronizationInstance))
    yield ()

  override def stop: UIO[Unit] =
    for
      maybeEdgeSynchronizationInstance <- edgeSynchronizationRef.getAndSet(None)
      edgeSynchronizationInstance <- maybeEdgeSynchronizationInstance.fold(
        ZIO.dieMessage("Graph is not started")
      ) (ZIO.succeed)
      // _ <- edgeSynchronizationInstance.stop // TODO: Implement stop to drain queue and shut down daemon
    yield ()

  private def getEdgeSynchronization: UIO[EdgeSynchronization] =
    for
      maybeEdgeSynchronization <- edgeSynchronizationRef.get
      edgeSynchronization <- maybeEdgeSynchronization.fold(
        ZIO.dieMessage("Graph has not been started")
      )(ZIO.succeed)
    yield edgeSynchronization

  override def get(
      id: NodeId
  ): NodeIO[Node] =
    for
      optionNode <- cache.get(id)
      node <- optionNode.fold(
        ZIO.scoped {
          for
            _ <- withNodePermit(id)
            optionNodeTestAgain <- cache.get(id) // check again since another thread may have cached this id
            node <- optionNodeTestAgain.fold(getNodeFromDataServiceAndAddToCache(id))(ZIO.succeed)
          yield node
        }
      )(ZIO.succeed)
    yield node

  private def getNodeFromDataServiceAndAddToCache(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    for
      node <- nodeRepositoryService.get(id)
      _ <- cache.put(node)
    yield node

  override def append(
      time: EventTime,
      changes: Vector[NodeMutationInput]
  ): NodeIO[Unit] =
    for
      output <- ZIO.foreachPar(changes)(processChangesForOneNode(time, _))

      edgeSynchronization <- getEdgeSynchronization
      _ <- edgeSynchronization.graphChanged(time, output)

      // Expect that all changes are far edge events or all not far edge events.
      // When the events don't include far edge events, this is an externally-driven mutation and needs
      // to be matched against standing queries. If the events are all far edge events, then there is no
      // need to match against standing queries since that will have been handled as part of the original
      // append operation that appended the (near) edges.
      _ <- (
        for
          // Expand the changed nodes to include the targets of any affected nodes
          affectedNodes <- allAffectedNodes(time, output)
          subgraphSpecs <- standingQueries.get
          _ <- ZIO.foreachParDiscard(subgraphSpecs)(matchAgainstStandingQueries(time, affectedNodes))
        yield ()
      ).when(changes.hasEventsOtherThanFarEdge)
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
        optionCachedNode <- cache.get(id)
        existingNode <- optionCachedNode.fold(getNodeFromDataServiceAndAddToCache(id))(ZIO.succeed)

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
        config = config.matcher,
        time = time,
        graph = this,
        tracing = tracing,
        subgraphSpec = subgraphSpec,
        nodes = changedNodes,
        contextStorage = contextStorage
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

type GraphEnvironment = AppConfig & NodeCache & NodeRepository & EdgeSynchronizationFactory & MatchSink & Tracing &
  ContextStorage

object Graph:
  val layer: URLayer[GraphEnvironment, Graph] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        nodeCache <- ZIO.service[NodeCache]
        nodeDataService <- ZIO.service[NodeRepository]
        edgeSynchronizationFactory <- ZIO.service[EdgeSynchronizationFactory]
        edgeSynchronizationRef <- Ref.make[Option[EdgeSynchronization]](None)
        matchSink <- ZIO.service[MatchSink]
        tracing <- ZIO.service[Tracing]
        contextStorage <- ZIO.service[ContextStorage]

        standingQueries <- Ref.make(Set.empty[SubgraphSpec])
        inFlight <- TSet.empty[NodeId].commit
      yield GraphLive(
        config = config,
        inFlight = inFlight,
        cache = nodeCache,
        nodeRepositoryService = nodeDataService,
        edgeSynchronizationFactory = edgeSynchronizationFactory,
        edgeSynchronizationRef = edgeSynchronizationRef,
        matchSink = matchSink,
        tracing = tracing,
        standingQueries = standingQueries,
        contextStorage = contextStorage
      )
    }
