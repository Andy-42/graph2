package andy42.graph.cache

import zio._
import zio.stm.STM
import zio.stm.TQueue
import andy42.graph.model.Node
import andy42.graph.model.EventsAtTime

/** Observe a node that is changed.
  */
trait StandingQueryEvaluation {

  /** A node has changed. The implementation will attempt to match all standing
    * queries using this node as a starting point.
    *
    * This node may have additional history appended to it that is not yet
    * persisted.
    * 
    * Since far edged might not be synced yet, when we pull in node for matching,
    * we can patch the far edges up to that they are consistent.
    */
  def nodeChanged(node: Node, newEvents: EventsAtTime): UIO[Unit]

  // TODO: How to get the output stream?
}

case class StandingQueryEvaluationLive(graph: Graph) extends StandingQueryEvaluation {
    override def nodeChanged(node: Node, newEvents: EventsAtTime): UIO[Unit] = ???
}

object StandingQueryEvaluation {
  val layer: URLayer[Graph, StandingQueryEvaluation] =
    ZLayer {
      for {
        graph <- ZIO.service[Graph]
        // TODO: Service that sinks the standing query output stream
      } yield StandingQueryEvaluationLive(graph)
    }
}
