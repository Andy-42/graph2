package andy42.graph.persistence

import andy42.graph.model.*
import zio.*
import zio.stream.Stream

import javax.sql.DataSource

trait EdgeReconciliationRepository:
  def markWindow(edgeReconciliation: EdgeReconciliationSnapshot): IO[PersistenceFailure, Unit]
  def contents: Stream[PersistenceFailure | UnpackFailure, EdgeReconciliationSnapshot]

  // TODO: Should include a timestamp of when the reconciliation was written

final case class EdgeReconciliationSnapshot(
    windowStart: Long,
    windowSize: Long,
    state: Byte
)

// TODO: Enum
object EdgeReconciliationSnapshot:
  private val Reconciled: Byte = 1.toByte
  private val Inconsistent: Byte = 2.toByte
  private val Unknown: Byte = 3.toByte

  /** All pairs of half-edges were determined to be reconciled for this window */
  def reconciled(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Reconciled)

  /** The window is known or suspected of being inconsistent */
  def inconsistent(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Inconsistent)

  /** The state is unknown. This is not typically something that we expect to write, and it would normally be
    * represented as a gap in the table.
    */
  def unknown(windowStart: Long, windowSize: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(windowStart, windowSize, Unknown)
