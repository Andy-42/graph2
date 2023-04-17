package andy42.graph.services

import andy42.graph.matcher.NodeMatches
import andy42.graph.model.EventTime
import zio.*
import zio.stream.ZStream

trait TestStandingQueryEvaluation:
  // Implicitly clears queue
  def graphChangedParameters: UIO[Chunk[(EventTime, Vector[NodeMutationOutput])]]

final case class TestStandingQueryEvaluationLive(queue: Queue[(EventTime, Vector[NodeMutationOutput])])
    extends StandingQueryEvaluation
    with TestStandingQueryEvaluation:

  override def graphChangedParameters: UIO[Chunk[(EventTime, Vector[NodeMutationOutput])]] =
    queue.takeAll

  override def output: Hub[SubgraphMatch] = ???

  override def graphChanged(time: EventTime, changes: Vector[NodeMutationOutput]): UIO[Unit] =
    queue.offer(time -> changes) *> ZIO.unit

object TestStandingQueryEvaluation:
  val layer: ULayer[StandingQueryEvaluation] =
    ZLayer {
      for queue <- Queue.unbounded[(EventTime, Vector[NodeMutationOutput])]
      yield TestStandingQueryEvaluationLive(queue)
    }
