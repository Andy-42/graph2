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
    windowStart: Long, // clustering key
    windowSize: Long, // payload
    state: Byte // payload
)

object EdgeReconciliation {
  val Reconciled: Byte = 1.toByte
  val Inconsistent: Byte = 2.toByte

  // All pairs of half-edges were determined to be reconciled for this window  
  def reconciled(windowStart: Long, windowSize: Long): EdgeReconciliation =
    new EdgeReconciliation(windowStart, windowSize, Reconciled)

  // The window is known or suspected of being inconsistent  
  def inconsistent(windowStart: Long, windowSize: Long): EdgeReconciliation =
    new EdgeReconciliation(windowStart, windowSize, Inconsistent)
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
      // TODO: Check that exactly one row is changed
    //   .retry(Schedule.exponential(10.millis) // .recurs(10)) // Do a bit of retrying, but give up eventually
      // TODO: Log retries, if the write eventually fails
     .foldZIO(_ => ZIO.unit, _ => ZIO.unit) // TODO: Is this the best way to consume errors
}

object EdgeReconciliationDataService {
  val layer: URLayer[DataSource, EdgeReconciliationDataService] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield EdgeReconciliationDataServiceLive(ds)
    }
}
