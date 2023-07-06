package andy42.graph.services

import andy42.graph.model.EventTime
import zio.*

case class TestEdgeSynchronizationFactoryLive() extends EdgeSynchronizationFactory:

  override def make(graph: Graph): UIO[EdgeSynchronization] =
    for queue <- Queue.unbounded[(EventTime, Vector[NodeMutationOutput])]
    yield TestEdgeSynchronizationLive(queue)

trait TestEdgeSynchronization:
  // Implicitly clears
  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[NodeMutationOutput])]]

final case class TestEdgeSynchronizationLive(queue: Queue[(EventTime, Vector[NodeMutationOutput])])
    extends EdgeSynchronization
    with TestEdgeSynchronization:

  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[NodeMutationOutput])]] =
    queue.takeAll

  def graphChanged(time: EventTime, changes: Vector[NodeMutationOutput]): UIO[Unit] =
    queue.offer(time -> changes).unit

  def startReconciliation: UIO[Unit] = ZIO.unit

object TestEdgeSynchronizationFactory:
  val layer: ULayer[TestEdgeSynchronizationFactoryLive] =
    ZLayer.succeed(TestEdgeSynchronizationFactoryLive())
