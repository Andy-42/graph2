package andy42.graph.cache

import andy42.graph.model._
import io.getquill._
import io.getquill.context.qzio.ImplicitSyntax._
import zio._

import javax.sql.DataSource

trait DataService {
  def runNodeHistory(id: NodeId): IO[ReadFailure, List[GraphHistory]]
  def runAppend(graphHistory: GraphHistory): IO[WriteFailure, Unit]
}

case class GraphHistory(
    id: NodeId, // clustering key
    eventTime: EventTime, // sort key
    sequence: Int, // sort key
    events: Array[Byte] // packed payload
)

case class DataServiceLive(ds: DataSource) extends DataService {

  val ctx = new PostgresZioJdbcContext(Literal)
  import ctx._

  inline def graph = quote { query[GraphHistory] }

  inline def nodeHistory(id: NodeId) = quote {
    graph
      .filter(_.id == lift(id))
      .sortBy(graphHistory => (graphHistory.eventTime, graphHistory.sequence))(
        Ord(Ord.asc, Ord.asc)
      )
  }

  inline def append(graphHistory: GraphHistory) = quote {
    graph
      .insertValue(lift(graphHistory))
      .onConflictUpdate(_.id, _.eventTime, _.sequence)((target, excluded) =>
        target.events -> excluded.events
      )
  }

  implicit val env: Implicit[DataSource] = Implicit(ds)

  override def runNodeHistory(id: NodeId): IO[ReadFailure, List[GraphHistory]] =
    run(nodeHistory(id))
      .mapError(SQLReadFailure(id, _))
      .implicitly
  override def runAppend(graphHistory: GraphHistory): IO[WriteFailure, Unit] =
    run(append(graphHistory))
      .mapError(SQLWriteFailure(graphHistory.id, _))
      .flatMap { rowsInsertedOrUpdated =>
        if (rowsInsertedOrUpdated != 1)
          ZIO.fail(
            CountPersistenceFailure(graphHistory.id, expected = 1, was = rowsInsertedOrUpdated)
          )
        else
          ZIO.unit
      }
      .implicitly
}

object DataService {
  val layer: URLayer[DataSource, DataService] =
    ZLayer {
      for {
        ds <- ZIO.service[DataSource]
      } yield DataServiceLive(ds)
    }
}
