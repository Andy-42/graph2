package andy42.graph.persistence

import andy42.graph.model.*
import andy42.graph.persistence.{NodeRepository, PersistenceFailure}
import zio.*
import zio.stm.TMap
import zio.stream.{UStream, ZStream}

trait TestNodeRepository:
  def clear(): UIO[Unit]

final case class TestNodeRepositoryLive(cache: TMap[(NodeId, EventTime, Int), EventsAtTime])
    extends NodeRepository
    with TestNodeRepository:

  override def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    for
      chunk <- cache.findAll { case entry @ ((id, _, _), _) => entry }.commit
      history = chunk.toVector
        .sortBy { case ((_, time, sequence), _) => (time, sequence) }
        .map(_._2)
    yield Node.fromHistory(id, history)

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    cache.put((id, eventsAtTime.time, eventsAtTime.sequence), eventsAtTime).commit

  override def contents: UStream[NodeRepositoryEntry] =
    ZStream.fromIterableZIO {
      for
        x <- cache.toChunk.commit
        nodeEntryChunk = x.map { case ((id, time, sequence), eventsAtTime) =>
          NodeRepositoryEntry(id = id, time = time, sequence = sequence, events = eventsAtTime.events)
        }.sorted
      yield nodeEntryChunk
    }

  override def clear(): UIO[Unit] = cache.removeIfDiscard((_, _) => true).commit

object TestNodeRepository:

  val layer: ULayer[NodeRepository] =
    ZLayer {
      for cache <- TMap.empty[(NodeId, EventTime, Int), EventsAtTime].commit
      yield TestNodeRepositoryLive(cache)
    }
