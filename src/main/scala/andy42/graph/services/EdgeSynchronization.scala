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
  def graphChanged(time: EventTime, changes: Vector[GroupedGraphMutationOutput]): UIO[Unit]

  def startReconciliation: UIO[Unit]

final case class EdgeReconciliationEvent(
    id: NodeId,
    time: EventTime,
    edge: Edge
):
  def edgeHash: Long = edge.hash(id)

final case class EdgeSynchronizationLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationService: EdgeReconciliationProcessor,
    graph: Graph,
    queue: Queue[EdgeReconciliationEvent]
) extends EdgeSynchronization:

  def graphChanged(time: EventTime, changes: Vector[GroupedGraphMutationOutput]): UIO[Unit] =
    for
      // Propagate the corresponding (near) half-edge to the other (far) node
      _ <- ZIO.foreach(changes) { mutationsForOneNode =>
        val id = mutationsForOneNode.node.id
        ZIO.foreach(mutationsForOneNode.events) {
          case Event.EdgeAdded(edge) => appendFarEdgeEvent(id, edge.other, time, Event.FarEdgeAdded(edge.reverse(id)))
          case Event.EdgeRemoved(edge) => appendFarEdgeEvent(id, edge.other, time, Event.FarEdgeRemoved(edge.reverse(id)))
          case _ => ZIO.unit
        }
      }

      _ <- ZIO.foreach(changes) { mutationsForOneNode =>
        val id = mutationsForOneNode.node.id
        val edgeReconciliationEvents = mutationsForOneNode.events.collect {
          case Event.EdgeAdded(edge)      => EdgeReconciliationEvent(id, time, edge)
          case Event.EdgeRemoved(edge)    => EdgeReconciliationEvent(id, time, edge)
          case Event.FarEdgeAdded(edge)   => EdgeReconciliationEvent(id, time, edge)
          case Event.FarEdgeRemoved(edge) => EdgeReconciliationEvent(id, time, edge)
        }

        if edgeReconciliationEvents.nonEmpty then queue.offerAll(edgeReconciliationEvents) else ZIO.unit
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
    */
  private def appendFarEdgeEvent(id: NodeId, other: NodeId, time: EventTime, event: Event): UIO[Unit] =
    import LogAnnotations.*
    graph
      .append(
        time,
        Vector(GraphMutationInput(id, event))
      ) // TODO: This may do redundant SQE evaluation - can SQE be bypassed?
      .catchAllCause(cause =>
        ZIO.logCause("Unexpected failure appending far edge event", cause)
        @@ operationAnnotation ("append far edge event")
        @@ timeAnnotation (time)
        @@ nearNodeIdAnnotation (id)
        @@ farNodeIdAnnotation (other)
        @@ eventAnnotation (event)
      )
      .forkDaemon *> ZIO.unit

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
        @@ operationAnnotation ("scan edge reconciliation event stream")
      )
      .forkDaemon *> ZIO.unit

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
