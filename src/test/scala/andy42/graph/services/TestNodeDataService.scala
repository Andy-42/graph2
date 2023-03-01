package andy42.graph.services

import andy42.graph.model._

import zio._
import zio.stm.TMap

final case class ForgetfulNodeDataService() extends NodeDataService:

  override def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    ZIO.succeed(Node.empty(id))

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    ZIO.unit

object ForgetfulNodeData:

  val layer: ULayer[NodeDataService] = ZLayer.succeed(ForgetfulNodeDataService())

final case class RememberingNodeDataService(cache: TMap[(NodeId, EventTime, Int), EventsAtTime])
    extends NodeDataService:

  override def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    for
      chunk <- cache.findAll { case entry @ ((id, _, _), _) => entry }.commit
      history = chunk.toVector
        .sortBy { case ((_, time, sequence), _) => (time, sequence) }
        .map(_._2)
    yield Node.fromHistory(id, history)

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    cache.put((id, eventsAtTime.time, eventsAtTime.sequence), eventsAtTime).commit

object RememberingNodeData:

  val layer: ULayer[NodeDataService] =
    ZLayer {
      for cache <- TMap.empty[(NodeId, EventTime, Int), EventsAtTime].commit
      yield RememberingNodeDataService(cache)
    }
