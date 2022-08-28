package andy42.graph.cache

import andy42.graph.model._
import io.getquill._
import io.getquill.jdbczio.Quill
import zio._

import java.sql.SQLException
import javax.sql.DataSource

sealed trait PersistenceFailure extends Throwable {
  def id: NodeId
}
trait ReadFailure extends PersistenceFailure
trait WriteFailure extends PersistenceFailure

trait SQLFailure {
  def ex: SQLException
}

case class SQLReadFailure(id: NodeId, ex: SQLException)
    extends ReadFailure
    with SQLFailure
case class SQLWriteFailure(id: NodeId, ex: SQLException)
    extends WriteFailure
    with SQLFailure

trait DataService {
  def runNodeHistory(id: NodeId): IO[ReadFailure, List[GraphHistory]]
  def runAppend(graphHistory: GraphHistory): IO[WriteFailure, Long]
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

  val dsLayer = ZLayer.succeed(ds)

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

  override def runNodeHistory(id: NodeId): IO[ReadFailure, List[GraphHistory]] =
    run(nodeHistory(id))
      .mapError(SQLReadFailure(id, _))
      .provideLayer(dsLayer)

  override def runAppend(graphHistory: GraphHistory): IO[WriteFailure, Long] =
    run(append(graphHistory))
      .mapError(SQLWriteFailure(graphHistory.id, _))
      .provideLayer(dsLayer)
}

object DataService {
  val layer: URLayer[DataSource, DataService] =
    ZLayer {
      for {
        ds <- ZIO.service[DataSource]
      } yield DataServiceLive(ds)
    }
}
