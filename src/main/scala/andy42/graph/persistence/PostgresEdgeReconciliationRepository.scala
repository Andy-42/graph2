package andy42.graph.persistence

import io.getquill.*
import io.getquill.context.qzio.ImplicitSyntax.*
import zio.*

import javax.sql.DataSource

final case class PostgresEdgeReconciliationRepositoryLive(ds: DataSource) extends EdgeReconciliationRepository:

  val ctx: PostgresZioJdbcContext[Literal] = PostgresZioJdbcContext(Literal)
  import ctx.*

  private inline def edgeReconciliationTable: Quoted[EntityQuery[EdgeReconciliationSnapshot]] = quote {
    query[EdgeReconciliationSnapshot]
  }

  private inline def quotedMarkWindow(
      edgeReconciliation: EdgeReconciliationSnapshot
  ): Quoted[Insert[EdgeReconciliationSnapshot]] = quote {
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

object PostgresEdgeReconciliationRepository:
  val layer: URLayer[DataSource, EdgeReconciliationRepository] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield PostgresEdgeReconciliationRepositoryLive(ds)
    }
