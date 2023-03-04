package andy42.graph.services

import andy42.graph.model.EventTime

import zio._

trait TestEdgeSynchronization:
  // Implicitly clears
  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[GroupedGraphMutationOutput])]]

final case class TestEdgeSynchronizationLive(queue: Queue[(EventTime, Vector[GroupedGraphMutationOutput])])
    extends EdgeSynchronization
    with TestEdgeSynchronization:

  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[GroupedGraphMutationOutput])]] =
    queue.takeAll

  def graphChanged(time: EventTime, changes: Vector[GroupedGraphMutationOutput]): UIO[Unit] =
    queue.offer(time -> changes) *> ZIO.unit

  def startReconciliation: UIO[Unit] = ZIO.unit

object TestEdgeSynchronization:
  val layer: ULayer[EdgeSynchronization] =
    ZLayer {
      for queue <- Queue.unbounded[(EventTime, Vector[GroupedGraphMutationOutput])]
      yield TestEdgeSynchronizationLive(queue)
    }
