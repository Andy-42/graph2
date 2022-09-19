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
  def eventsAppended(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): UIO[Unit]

  def startReconciliation: UIO[Unit]

final case class EdgeReconciliationEvent(
    id: NodeId,
    time: EventTime,
    edge: Edge
):
  def edgeHash: Long = edge.hash(id)

final case class EdgeSynchronizationLive(
    config: EdgeReconciliationConfig,
    edgeReconciliationService: EdgeReconciliationService,
    clock: Clock,
    graph: Graph,
    queue: Queue[EdgeReconciliationEvent]
) extends EdgeSynchronization:

  def eventsAppended(
      id: NodeId,
      time: EventTime,
      events: Vector[Event]
  ): UIO[Unit] =
    for
      _ <- ZIO.foreach(events) { // Propagate the corresponding half-edge to the other node

        case Event.EdgeAdded(edge) =>
          graph
            .append(edge.other, time, Vector(Event.FarEdgeAdded(edge.reverse(id))))
            .fork // TODO: Handle fiber exit

        case Event.EdgeRemoved(edge) =>
          graph
            .append(edge.other, time, Vector(Event.FarEdgeRemoved(edge.reverse(id))))
            .fork // TODO: Handle fiber exit

        case _ => ZIO.unit
      }

      _ <- queue.offerAll(events.collect {
        case Event.EdgeAdded(edge)      => EdgeReconciliationEvent(id, time, edge)
        case Event.EdgeRemoved(edge)    => EdgeReconciliationEvent(id, time, edge)
        case Event.FarEdgeAdded(edge)   => EdgeReconciliationEvent(id, time, edge)
        case Event.FarEdgeRemoved(edge) => EdgeReconciliationEvent(id, time, edge)
      })
    yield ()

  def startReconciliation: UIO[Unit] =
    ZStream
      .fromQueue(queue, maxChunkSize = config.maxChunkSize)
      // TODO: Ensure fires at regular intervals - zip with something using config.maximumIntervalBetweenChunks
      .chunks
      .scanZIO(edgeReconciliationService.zero)((state, chunk) => edgeReconciliationService.addChunk(state, chunk))
      .runDrain
      .fork *> ZIO.unit // TODO: Error handling if fiber dies

object EdgeSynchronization:

  val layer: URLayer[EdgeReconciliationConfig & Graph & Clock & EdgeReconciliationService, EdgeSynchronization] =
    ZLayer {
      for
        config <- ZIO.service[EdgeReconciliationConfig]
        edgeReconciliationService <- ZIO.service[EdgeReconciliationService]
        clock <- ZIO.service[Clock]
        graph <- ZIO.service[Graph]
        queue <- Queue.unbounded[EdgeReconciliationEvent] // TODO: s/b bounded?
        live = EdgeSynchronizationLive(config, edgeReconciliationService, clock, graph, queue)
        _ <- live.startReconciliation
      yield live
    }
