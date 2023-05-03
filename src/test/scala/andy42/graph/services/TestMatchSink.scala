package andy42.graph.services
import zio.*

trait TestMatchSink:
  def matches: UIO[Chunk[SubgraphMatchAtTime]]

case class TestMatchSinkLive(queue: Queue[SubgraphMatchAtTime]) extends MatchSink with TestMatchSink:
  override def matches: UIO[Chunk[SubgraphMatchAtTime]] = queue.takeAll

  override def offer(matches: SubgraphMatchAtTime): UIO[Unit] =
    ZIO.debug(matches) *> queue.offer(matches).unit

object TestMatchSink:

  val layer: ULayer[MatchSink] =
    ZLayer {
      for queue <- Queue.unbounded[SubgraphMatchAtTime]
      yield TestMatchSinkLive(queue)
    }
