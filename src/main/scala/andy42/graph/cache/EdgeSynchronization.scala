package andy42.graph.cache

import andy42.graph.cache.EdgeReconciliationConfig
import andy42.graph.model._
import zio._
import zio.stm._
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

trait EdgeSynchronization:

  /** Ensure any far edges are synchronized. When an edge is set, a [[NearEdgeEvent]] that represents a half-edge that
    * is appended to the (near) node. This service will eventually append a [[FarEdgeEvent]] to the other (far) node
    * that corresponds to the other half-edge.
    *
    * Propagating the edges to the far nodes is necessarily asynchronous and eventually consistent due to the lock-free
    * graph implementation. If a synchronized graph was used, it would be easy to construct a scenario where deadlock is
    * possible. Since the we only have eventual consistency, the EdgeSynchronization implementation should monitor (and
    * perhaps repair) edge consistency.
    */
  def eventsAppended(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): URIO[Clock, Unit]

  def startReconciliation(config: EdgeReconciliationConfig): URIO[Clock & EdgeReconciliationDataService, Unit]

final case class EdgeReconciliationEvent(
    id: NodeId,
    atTime: EventTime,
    event: EdgeEvent
):
  def edgeHash: Long = event.edge.edgeHash(id)

final case class EdgeSynchronizationLive(
    graph: Graph,
    queue: Queue[EdgeReconciliationEvent]
) extends EdgeSynchronization:

  def eventsAppended(
      id: NodeId,
      atTime: EventTime,
      events: Vector[Event]
  ): URIO[Clock, Unit] =
    for
      _ <- ZIO.foreach(events) {

        case EdgeAdded(edge) =>
          graph
            .append(edge.other, atTime, Vector(FarEdgeAdded(edge.reverse(id))))
            .fork // TODO: Handle fiber exit

        case EdgeRemoved(edge) =>
          graph
            .append(edge.other, atTime, Vector(FarEdgeRemoved(edge.reverse(id))))
            .fork // TODO: Handle fiber exit

        case _ => ZIO.unit
      }
      _ <- ZIO.foreach(events) {
        case edgeEvent: EdgeEvent =>
          queue.offer(EdgeReconciliationEvent(id, atTime, edgeEvent))

        case _ => ZIO.unit
      }
    yield ()

  // TODO: Need unit tests on hash reconciliation scheme to show algebra works

  def startReconciliation(config: EdgeReconciliationConfig): URIO[Clock & EdgeReconciliationDataService, Unit] =
    ZStream
      .fromQueue(queue, maxChunkSize = config.maxChunkSize)
      // TODO: Ensure fires at regular intervals - zip with something using config.maximumIntervalBetweenChunks
      .chunks
      .scanZIO(EdgeReconciliationState(config))((s, c) => s.addChunk(c))
      .runDrain
      .fork *> ZIO.unit // TODO: Error handling if fiber dies

object EdgeSynchronization:

  val layer: URLayer[Graph & Clock & EdgeReconciliationConfig & EdgeReconciliationDataService, EdgeSynchronization] =
    ZLayer {
      for
        config <- ZIO.service[EdgeReconciliationConfig]
        graph <- ZIO.service[Graph]
        queue <- Queue.unbounded[EdgeReconciliationEvent]
        live = EdgeSynchronizationLive(graph, queue)
        _ <- live.startReconciliation(config)
      yield live
    }
