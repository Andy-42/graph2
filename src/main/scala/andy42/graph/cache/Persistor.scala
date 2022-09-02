package andy42.graph.cache

import andy42.graph.model._
import io.getquill._
import zio._

import java.sql.SQLException

trait Persistor {

  /** Get the full history of the node. The result will be in ascending order of
    * (eventTime, sequence).
    */
  def get(id: NodeId): IO[ReadFailure | UnpackFailure, Vector[EventsAtTime]]

  /** Append an EventsAtTime to a node's persisted history. Within the
    * historyfor a Node, the (eventTime, sequence) must be unique. Within an
    * eventTime, the sequence numbers should be a dense sequence starting at
    * zero.
    */
  def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[PersistenceFailure, Unit]
}

case class PersistorLive(dataService: NodeDataService) extends Persistor {

  override def get(
      id: NodeId
  ): IO[ReadFailure | UnpackFailure, Vector[EventsAtTime]] =
    dataService
      .runNodeHistory(id)
      .flatMap { (history: List[GraphHistory]) =>
        ZIO.foreach(history.toVector)(PersistenceConversions.toEventsAtTime)
      }

  override def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[WriteFailure, Unit] =
    dataService.runAppend(PersistenceConversions.toGraphHistory(id, eventsAtTime))
}

object Persistor {
  val layer: URLayer[NodeDataService, Persistor] =
    ZLayer {
      for {
        dataService <- ZIO.service[NodeDataService]
      } yield PersistorLive(dataService)
    }
}
