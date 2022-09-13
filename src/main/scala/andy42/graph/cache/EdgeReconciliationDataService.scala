package andy42.graph.cache

import andy42.graph.model.Edge
import andy42.graph.model.NodeId
import io.getquill._
import io.getquill.context.qzio.ImplicitSyntax._
import zio._

import javax.sql.DataSource


trait EdgeReconciliationDataService:
  def markWindow(edgeReconciliation: EdgeReconciliation): UIO[Unit]
  
final case class EdgeReconciliation(
    windowStart: Long, // clustering key
    windowSize: Long, // payload
    state: Byte // payload
)

// TODO: Smells like an enum
object EdgeReconciliation:
  val Reconciled: Byte = 1.toByte
  val Inconsistent: Byte = 2.toByte

  // All pairs of half-edges were determined to be reconciled for this window
  def reconciled(windowStart: Long, windowSize: Long): EdgeReconciliation =
    EdgeReconciliation(windowStart, windowSize, Reconciled)

  // The window is known or suspected of being inconsistent
  def inconsistent(windowStart: Long, windowSize: Long): EdgeReconciliation =
    EdgeReconciliation(windowStart, windowSize, Inconsistent)

final case class EdgeReconciliationDataServiceLive(ds: DataSource) extends EdgeReconciliationDataService:

  val ctx = PostgresZioJdbcContext(Literal)
  import ctx._

  inline def edgeReconciliationTable = quote { query[EdgeReconciliation] }

  inline def quotedMarkWindow(edgeReconciliation: EdgeReconciliation) = quote {
    edgeReconciliationTable
      .insertValue(lift(edgeReconciliation))
      .onConflictUpdate(_.windowStart)(
        (table, excluded) => table.windowSize -> excluded.windowSize,
        (table, excluded) => table.state -> excluded.state
      )
  }

  given Implicit[DataSource] = Implicit(ds)

  override def markWindow(edgeReconciliation: EdgeReconciliation): UIO[Unit] =
    run(quotedMarkWindow(edgeReconciliation)).implicitly
      .retry(Schedule.recurs(5)) // TODO: Configure retry policy
      // TODO: Check that exactly one row is changed, otherwise log error
      // TODO: Log if retries fails
      .foldZIO(_ => ZIO.unit, _ => ZIO.unit) // TODO: Is this the best way to consume errors

object EdgeReconciliationDataService:
  val layer: URLayer[DataSource, EdgeReconciliationDataService] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield EdgeReconciliationDataServiceLive(ds)
    }
