package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.stm.*
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

trait EdgeSynchronization:

  /** Ensure any far edges are synchronized. When an edge is set, a near edge event that represents a half-edge that is
    * appended to the (near) node. This service will eventually append a far edge event to the other (far) node that
    * corresponds to the other half-edge.
    *
    * Propagating the edges to the far nodes is necessarily asynchronous and eventually consistent due to the lock-free
    * graph implementation. If a synchronized graph was used, it would be easy to construct a scenario where deadlock is
    * possible. Since the we only have eventual consistency, the EdgeSynchronization implementation should monitor (and
    * perhaps repair) edge consistency.
    */
  def graphChanged(time: EventTime, changes: Vector[NodeMutationOutput]): UIO[Unit]

  def startReconciliation: UIO[Unit]

final case class EdgeReconciliationEvent(
    id: NodeId,
    time: EventTime,
    edge: Edge
):
  def edgeHash: Long = edge.hash(id)

extension (mutations: Vector[NodeMutationOutput])
  def extractOtherIdFromNearEdgeEvents: Vector[NodeId] =
    for
      groupedGraphMutationOutput <- mutations
      other <- groupedGraphMutationOutput.events.collect {
        case Event.EdgeAdded(edge: NearEdge)   => edge.other
        case Event.EdgeRemoved(edge: NearEdge) => edge.other
      }
    yield other

extension (mutations: NodeMutationInput)
  def extractOtherIdsNodeFromFarEdgeEvents: Vector[NodeId] =
    for other <- mutations.events.collect {
        case Event.FarEdgeAdded(edge)   => edge.other
        case Event.FarEdgeRemoved(edge) => edge.other
      }
    yield other

final case class EdgeSynchronizationLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationService: EdgeReconciliationProcessor,
    graph: Graph,
    queue: Queue[EdgeReconciliationEvent]
) extends EdgeSynchronization:

  def graphChanged(time: EventTime, nodeMutations: Vector[NodeMutationOutput]): UIO[Unit] =

    def gatherReversedEdgeEventsForOtherNode(other: NodeId): NodeMutationInput =
      NodeMutationInput(
        id = other,
        events = for
          groupedGraphMutationOutput <- nodeMutations
          id = groupedGraphMutationOutput.node.id
          reversedEdgeEvent <- groupedGraphMutationOutput.events.collect {
            case Event.EdgeAdded(edge: NearEdge)   => Event.FarEdgeAdded(edge.reverse(id))
            case Event.EdgeRemoved(edge: NearEdge) => Event.FarEdgeRemoved(edge.reverse(id))
          }
        yield reversedEdgeEvent
      )

    for
      _ <- ZIO.foreachDiscard(nodeMutations.extractOtherIdFromNearEdgeEvents.toSet) { other =>
        appendFarEdgeEventsFor(id = other, time = time, events = gatherReversedEdgeEventsForOtherNode(other))
      }

      _ <- ZIO.foreachDiscard(nodeMutations) { nodeMutationOutput =>
        val id = nodeMutationOutput.node.id
        val edgeReconciliationEvents = nodeMutationOutput.events.collect {
          case Event.EdgeAdded(edge)      => EdgeReconciliationEvent(id, time, edge)
          case Event.EdgeRemoved(edge)    => EdgeReconciliationEvent(id, time, edge)
          case Event.FarEdgeAdded(edge)   => EdgeReconciliationEvent(id, time, edge)
          case Event.FarEdgeRemoved(edge) => EdgeReconciliationEvent(id, time, edge)
        }

        queue.offerAll(edgeReconciliationEvents).when(edgeReconciliationEvents.nonEmpty)
      }
    yield ()

  /** Append a FarEdgeAdded or FarEdgeRemoved to the other node.
    *
    * An edge is changed when an EdgeAdded or EdgeRemoved is appended to node `id`. A full edge is composed of a pair of
    * half-edges, with each pair of edges reversed. This function appends the reversed edge to the other (far) node,
    * making the edge whole.
    *
    * Appending the reversed edge to the far node is necessarily done outside of holding the node permit to avoid a
    * communications deadlock. In this implementation, appending the reverse edge is done on a daemon fiber that may
    * fail, and thereby cause an inconsistency in the graph (i.e., a missing half-edge).
    *
    * The Graph.append method will not run SQE if all the events being appended are far edges since that processing will
    * always have been done in the contents of the original append.
    */
  private def appendFarEdgeEventsFor(
      id: NodeId,
      time: EventTime,
      events: NodeMutationInput
  ): UIO[Unit] =
    import LogAnnotations.*
    graph
      .appendFarEdgeEvents(time, events)
      .catchAllCause(cause =>
        ZIO.logCause("Unexpected failure appending far edge event", cause)
          @@ operationAnnotation("append far edge events")
          @@ timeAnnotation(time)
          @@ nearNodeIdAnnotation(id)
          @@ farNodeIdsAnnotation(events.extractOtherIdsNodeFromFarEdgeEvents)
      )
      .forkDaemon.unit

  /** Start the fiber that consumes the edge synchronization events and reconciles them.
    *
    * TODO: If this daemon fiber fails, should it be restarted?
    */
  def startReconciliation: UIO[Unit] =
    import LogAnnotations.operationAnnotation
    ZStream
      .fromQueue(queue, maxChunkSize = config.maxChunkSize)
      // Ensure that a chunk is processed at regular intervals for window expiry processing
      .groupedWithin(chunkSize = config.maxChunkSize, within = config.maximumIntervalBetweenChunks)
      // For each chunk processed, add new events into time window reconciliations, and report on state of expired windows
      .scanZIO(edgeReconciliationService.zero)((state, chunk) => edgeReconciliationService.addChunk(state, chunk))
      .runDrain
      .catchAllCause(cause =>
        ZIO.logCause("Unexpected failure scanning edge reconciliation event stream", cause)
          @@ operationAnnotation("scan edge reconciliation event stream")
      )
      .forkDaemon.unit

object EdgeSynchronization:

  val layer: URLayer[EdgeReconciliationConfig & Graph & EdgeReconciliationProcessor, EdgeSynchronization] =
    ZLayer {
      for
        config <- ZIO.service[EdgeReconciliationConfig]
        edgeReconciliationService <- ZIO.service[EdgeReconciliationProcessor]
        graph <- ZIO.service[Graph]
        queue <- Queue.unbounded[EdgeReconciliationEvent] // TODO: s/b bounded?
        live = EdgeSynchronizationLive(config, edgeReconciliationService, graph, queue)
        _ <- live.startReconciliation
      yield live
    }
