package andy42.graph.services

import andy42.graph.model.Edge
import andy42.graph.model.NodeId
import io.getquill.*
import io.getquill.context.qzio.ImplicitSyntax.*
import zio.*

import javax.sql.DataSource

trait EdgeReconciliationRepository:
  def markWindow(edgeReconciliation: EdgeReconciliationSnapshot): UIO[Unit]

final case class EdgeReconciliationSnapshot private (
    windowStart: Long, // clustering key
    windowSize: Long, // payload
    state: Byte // payload
)

object EdgeReconciliationSnapshot:
  private val Reconciled: Byte = 1.toByte
  private val Inconsistent: Byte = 2.toByte
  private val Unknown: Byte = 3.toByte

  // All pairs of half-edges were determined to be reconciled for this window
  def reconciled(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Reconciled)

  // The window is known or suspected of being inconsistent
  def inconsistent(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Inconsistent)

  // The state is unknown. This is not typically something that we expect to write,
  // and it would normally be represented as a gap in the table.
  def unknown(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Unknown)

final case class EdgeReconciliationRepositoryLive(ds: DataSource) extends EdgeReconciliationRepository:

  val ctx: PostgresZioJdbcContext[Literal] = PostgresZioJdbcContext(Literal)
  import ctx.*

  private inline def edgeReconciliationTable: Quoted[EntityQuery[EdgeReconciliationSnapshot]] = quote { 
    query[EdgeReconciliationSnapshot] 
  }

  private inline def quotedMarkWindow(edgeReconciliation: EdgeReconciliationSnapshot): Quoted[Insert[EdgeReconciliationSnapshot]] = quote {
    edgeReconciliationTable
      .insertValue(lift(edgeReconciliation))
      .onConflictUpdate(_.windowStart)(
        (table, excluded) => table.windowSize -> excluded.windowSize,
        (table, excluded) => table.state -> excluded.state
      )
  }

  given Implicit[DataSource] = Implicit(ds)

  override def markWindow(edgeReconciliation: EdgeReconciliationSnapshot): UIO[Unit] =
    run(quotedMarkWindow(edgeReconciliation)).implicitly
      .retry(Schedule.recurs(5)) // TODO: Configure retry policy - either exponential or fibonacci
      // TODO: Check that exactly one row is changed, otherwise log error
      // TODO: Log if retries fails
      .foldZIO(_ => ZIO.unit, _ => ZIO.unit) // TODO: Is this the best way to consume errors

object EdgeReconciliationRepository:
  val layer: URLayer[DataSource, EdgeReconciliationRepository] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield EdgeReconciliationRepositoryLive(ds)
    }
