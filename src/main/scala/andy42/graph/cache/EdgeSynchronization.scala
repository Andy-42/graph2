package andy42.graph.cache

import andy42.graph.model._
import zio._
import zio.stm._
import zio.stream.ZStream

import java.time.temporal.ChronoUnit.MILLIS

trait EdgeSynchronization {

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
      eventsAtTime: EventsAtTime
  ): UIO[Unit]

  def startReconciliation(state: ReconciliationState): URIO[Clock, Unit]
}

case class EdgeReconciliationEvent(
    id: NodeId,
    atTime: EventTime,
    event: EdgeEvent
) {
  def edgeHash: Long = event.edge.edgeHash(id)
}

case class EdgeSynchronizationLive(
    graph: Graph,
    queue: Queue[EdgeReconciliationEvent]
) extends EdgeSynchronization {

  def eventsAppended(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): UIO[Unit] =
    for {
      _ <- ZIO
        .foreach(eventsAtTime.events) { event =>
          propagateEdgeEventsToFarNode(id, eventsAtTime.eventTime, event)
        }
      _ <- ZIO
        .foreach(eventsAtTime.events) { event =>
          reconcileEvents(id, eventsAtTime.eventTime, event)
        }
    } yield ()

  def propagateEdgeEventsToFarNode(
      id: NodeId,
      atTime: EventTime,
      event: Event
  ): UIO[Unit] = event match {
    case EdgeAdded(edge) =>
      graph
        .append(id, atTime, Vector(FarEdgeAdded(edge.reverse(id))))
        .mapError { e =>
          handleFailureToPropagateEdgeToFarNode(id, atTime, event, e)
        }
        .fork *> ZIO.unit
    case EdgeRemoved(edge) =>
      graph
        .append(id, atTime, Vector(FarEdgeRemoved(edge.reverse(id))))
        .mapError { e =>
          handleFailureToPropagateEdgeToFarNode(id, atTime, event, e)
        }
        .fork *> ZIO.unit
    case _ => ZIO.unit
  }

  def handleFailureToPropagateEdgeToFarNode(
      id: NodeId,
      atTime: EventTime,
      event: Event,
      e: UnpackFailure | PersistenceFailure
  ): UIO[Unit] = ZIO.unit // TODO: Do some logging

  def reconcileEvents(id: NodeId, atTime: EventTime, event: Event): UIO[Unit] =
    event match {
      case e: EdgeEvent =>
        queue.offer(EdgeReconciliationEvent(id, atTime, e)) *> ZIO.unit
      case _ => ZIO.unit
    }

  // TODO: Need unit tests on hash reconciliation scheme to show algebra works

  // TODO: Need window size from config

  // Window size parameters here; Require Log
  def startReconciliation(state: ReconciliationState): URIO[Clock, Unit] =
    ZStream
      .fromQueue(queue, 1000 /* TODO: maxChunkSize from config */ )
      // TODO: Ensure fires at regular intervals - zip with something
      .chunks
      .scanZIO(state)((s, c) => s.addChunk(c))
      .runDrain
      .fork *> ZIO.unit // TODO: Error handling if fiber dies
}

object EdgeSynchronization {

  val layer: URLayer[Graph & Clock, EdgeSynchronization] =
    ZLayer {
      for {
        graph <- ZIO.service[Graph]
        queue <- Queue.unbounded[EdgeReconciliationEvent]
        live = EdgeSynchronizationLive(graph, queue)
        windowSize: Duration = ??? // TODO: from config
        windowExpiry: Duration = ??? // TODO: from config
        _ <- live.startReconciliation(
          ReconciliationState(
            windowSize = windowSize.get(MILLIS),
            windowExpiry = windowExpiry.get(MILLIS)
          )
        )
      } yield live
    }
}
