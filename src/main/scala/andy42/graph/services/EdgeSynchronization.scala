package andy42.graph.services

import andy42.graph.model.*
import zio.*
import zio.logging.LogAnnotation
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

        case Event.EdgeAdded(edge)   => appendFarEvent(id, edge.other, time, Event.FarEdgeAdded(edge.reverse(id)))
        case Event.EdgeRemoved(edge) => appendFarEvent(id, edge.other, time, Event.FarEdgeRemoved(edge.reverse(id)))
        case _                       => ZIO.unit
      }

      _ <- queue.offerAll(events.collect {
        case Event.EdgeAdded(edge)      => EdgeReconciliationEvent(id, time, edge)
        case Event.EdgeRemoved(edge)    => EdgeReconciliationEvent(id, time, edge)
        case Event.FarEdgeAdded(edge)   => EdgeReconciliationEvent(id, time, edge)
        case Event.FarEdgeRemoved(edge) => EdgeReconciliationEvent(id, time, edge)
      })
    yield ()

  private def appendFarEvent(id: NodeId, other: NodeId, time: EventTime, event: Event): UIO[Unit] =
    import EdgeSynchronizationLogAnnotations._
    val operation = graph.append(other, time, Vector(event))
    @@ operationAnnotation ("append far event")
    @@ timeAnnotation (time)
    @@ nearNodeIdAnnotation (id)
    @@ farNodeIdAnnotation (other)
    @@ eventAnnotation (event)

    operation.forkDaemon *> ZIO.unit

  def startReconciliation: UIO[Unit] =
    import EdgeSynchronizationLogAnnotations.operationAnnotation
    val operation = ZStream
      .fromQueue(queue, maxChunkSize = config.maxChunkSize)
      // Ensure that a chunk is processed at regular intervals for window expiry processing
      .groupedWithin(chunkSize = config.maxChunkSize, within = config.maximumIntervalBetweenChunks)
      // For each chunk processed, add new events into running summaries and report on state of exired windows
      .scanZIO(edgeReconciliationService.zero)((state, chunk) => edgeReconciliationService.addChunk(state, chunk))
      .runDrain
      @@ operationAnnotation("scan reconciliation event stream")

    operation.forkDaemon *> ZIO.unit

object EdgeSynchronizationLogAnnotations:

  def formatNodeId(id: NodeId): String = id.map(_.toInt.toHexString).mkString
  def formatEvent(event: Event): String = event.getClass.getSimpleName

  val operationAnnotation = LogAnnotation[String]("operation", (_, x) => x, _.toString)
  val nearNodeIdAnnotation = LogAnnotation[NodeId]("nearNodeId", (_, x) => x, formatNodeId)
  val farNodeIdAnnotation = LogAnnotation[NodeId]("farNodeId", (_, x) => x, formatNodeId)
  val eventAnnotation = LogAnnotation[Event]("event", (_, x) => x, formatEvent)
  val timeAnnotation = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)

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
