package andy42.graph.cache

import andy42.graph.model.NodeId
import io.getquill._
import io.getquill.context.qzio.ImplicitSyntax._
import zio._

import javax.sql.DataSource
import andy42.graph.model.Edge

// enum EdgeReconciliationState(value: Byte):
//   case Unknown extends EdgeReconciliationState(0.toByte)
//   case Reconciled extends EdgeReconciliationState(1.toByte)
//   case Broken extends EdgeReconciliationState(2.toByte)

case class EdgeReconciliation(
    windowStart: Long, // sort key
    windowSize: Long,
    state: Byte
)

object EdgeReconciliation {
  val Unknown: Byte = 0.toByte
  val Reconciled: Byte = 1.toByte
  val Broken: Byte = 2.toByte

  def unknown(windowStart: Long, windowSize: Long): EdgeReconciliation =
    new EdgeReconciliation(windowStart, windowSize, Unknown)

  def reconciled(windowStart: Long, windowSize: Long): EdgeReconciliation =
    new EdgeReconciliation(windowStart, windowSize, Reconciled)

  def broken(windowStart: Long, windowSize: Long): EdgeReconciliation =
    new EdgeReconciliation(windowStart, windowSize, Broken)
}

trait EdgeReconciliationDataService {
  def runMarkWindow(edgeReconciliation: EdgeReconciliation): UIO[Unit]
}

case class EdgeReconciliationDataServiceLive(ds: DataSource) extends EdgeReconciliationDataService {

  val ctx = new PostgresZioJdbcContext(Literal)
  import ctx._

  inline def edgeReconciliationTable = quote { query[EdgeReconciliation] }

  inline def markWindow(edgeReconciliation: EdgeReconciliation) = quote {
    edgeReconciliationTable
      .insertValue(lift(edgeReconciliation))
      .onConflictUpdate(_.windowStart)(
        (table, excluded) => table.windowSize -> excluded.windowSize,
        (table, excluded) => table.state -> excluded.state
      )
  }

  implicit val env: Implicit[DataSource] = Implicit(ds)

  def runMarkWindow(edgeReconciliation: EdgeReconciliation): UIO[Unit] =
    run(markWindow(edgeReconciliation)).implicitly
      .foldZIO(_ => ZIO.unit, _ => ZIO.unit) // TODO: Retry and error handling
}

object EdgeReconciliationDataService {
  val layer: URLayer[DataSource, EdgeReconciliationDataService] =
    ZLayer {
      for {
        ds <- ZIO.service[DataSource]
      } yield EdgeReconciliationDataServiceLive(ds)
    }
}
