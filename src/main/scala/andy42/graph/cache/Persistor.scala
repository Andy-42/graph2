package andy42.graph.cache

import andy42.graph.model._

import zio._
import java.sql.SQLException
import io.getquill.jdbczio.Quill
import io.getquill._
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
  ): IO[PersistenceFailure, Long]
}

object Conversion {
  def toEventsAtTime(
      graphHistory: GraphHistory
  ): IO[UnpackFailure, EventsAtTime] =
    for {
      events <- Events.unpack(graphHistory.events)
    } yield EventsAtTime(
      eventTime = graphHistory.eventTime,
      sequence = graphHistory.sequence,
      events = events
    )

  def toGraphHistory(id: NodeId, eventsAtTime: EventsAtTime): GraphHistory =
    GraphHistory(
      id = id,
      eventTime = eventsAtTime.eventTime,
      sequence = eventsAtTime.sequence,
      events = Events.pack(eventsAtTime.events)
    )
}

case class PersistorLive(dataService: DataService) extends Persistor {

  import Conversion._

  override def get(
      id: NodeId
  ): IO[ReadFailure | UnpackFailure, Vector[EventsAtTime]] =
    dataService
      .runNodeHistory(id)
      .flatMap((history: List[GraphHistory]) =>
        ZIO.foreach(history.toVector)(toEventsAtTime)
      )

  override def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[WriteFailure, Long] =
    dataService.runAppend(toGraphHistory(id, eventsAtTime))
}

object Persistor {
  val layer: URLayer[DataService, Persistor] =
    ZLayer {
      for {
        dataService <- ZIO.service[DataService]
      } yield PersistorLive(dataService)
    }
}
