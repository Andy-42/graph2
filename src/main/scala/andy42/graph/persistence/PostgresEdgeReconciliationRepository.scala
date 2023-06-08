package andy42.graph.persistence

import andy42.graph.model.UnpackFailure
import io.getquill.*
import io.getquill.context.qzio.ImplicitSyntax.*
import zio.*
import zio.stream.Stream

import java.sql.SQLException
import javax.sql.DataSource

final case class PostgresEdgeReconciliationRepositoryLive(ds: DataSource) extends EdgeReconciliationRepository:

  val ctx: PostgresZioJdbcContext[Literal] = PostgresZioJdbcContext(Literal)
  import ctx.*

  implicit val reconciliationStateDecoder: MappedEncoding[EdgeReconciliationState, Byte] =
    MappedEncoding[EdgeReconciliationState, Byte](_.ordinal.toByte)
  implicit val reconciliationStateEncoder: MappedEncoding[Byte, EdgeReconciliationState] =
    MappedEncoding[Byte, EdgeReconciliationState]((x: Byte) => EdgeReconciliationState.fromOrdinal(x.toInt))

  private inline def edgeReconciliationTable: Quoted[EntityQuery[EdgeReconciliationSnapshot]] = quote {
    query[EdgeReconciliationSnapshot]
  }

  private inline def contentsQuery: Quoted[Query[EdgeReconciliationSnapshot]] = quote {
    query[EdgeReconciliationSnapshot].sortBy(_.windowStart)
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

  override def markWindow(
      edgeReconciliation: EdgeReconciliationSnapshot
  ): IO[SQLEdgeReconciliationMarkWindowFailure, Unit] =
    run(quotedMarkWindow(edgeReconciliation)).implicitly
      .refineOrDie { case e: SQLException =>
        SQLEdgeReconciliationMarkWindowFailure(
          windowStart = edgeReconciliation.windowStart,
          windowSize = edgeReconciliation.windowSize,
          state = edgeReconciliation.state.ordinal.toByte,
          ex = e
        )
      }
      .retry(Schedule.recurs(5)) // TODO: Configure retry policy - either exponential or fibonacci
      // TODO: Check that exactly one row is changed, otherwise log error
      // TODO: Log if retries fails
      .unit

  def contents: Stream[PersistenceFailure | UnpackFailure, EdgeReconciliationSnapshot] =
    stream(contentsQuery).implicitly
      .refineOrDie { case e: SQLException => SQLEdgeReconciliationContentsFailure(e) }

object PostgresEdgeReconciliationRepository:
  val layer: URLayer[DataSource, EdgeReconciliationRepository] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield PostgresEdgeReconciliationRepositoryLive(ds)
    }
