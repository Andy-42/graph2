package andy42.graph.services

import andy42.graph.model.EventTime

import zio.*

trait TestStandingQueryEvaluation:
  // Implicitly clears queue
  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[GroupedGraphMutationOutput])]]

final case class TestStandingQueryEvaluationLive(queue: Queue[(EventTime, Vector[GroupedGraphMutationOutput])])
    extends StandingQueryEvaluation
    with TestStandingQueryEvaluation:

  override def graphChangedParameters: UIO[Chunk[(EventTime, Vector[GroupedGraphMutationOutput])]] =
    queue.takeAll

  override def graphChanged(time: EventTime, changes: Vector[GroupedGraphMutationOutput]): UIO[Unit] =
    queue.offer(time -> changes) *> ZIO.unit

object TestStandingQueryEvaluation:
  val layer: ULayer[StandingQueryEvaluation] =
    ZLayer {
      for queue <- Queue.unbounded[(EventTime, Vector[GroupedGraphMutationOutput])]
      yield TestStandingQueryEvaluationLive(queue)
    }
