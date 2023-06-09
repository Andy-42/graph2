package andy42.graph.persistence

import andy42.graph.model.*
import zio.*
import zio.stream.Stream

import javax.sql.DataSource

// TODO: Rename to EdgeReconciliationEntry
final case class EdgeReconciliationSnapshot(
    windowStart: Long, // epoch millis
    windowSize: Long, // millis
    whenWritten: Long, // epoch millis
    state: EdgeReconciliationState
)

enum EdgeReconciliationState:
  case Reconciled, Inconsistent, Unknown

trait EdgeReconciliationRepository:
  def markWindow(edgeReconciliation: EdgeReconciliationSnapshot): IO[PersistenceFailure, Unit]
  def contents: Stream[PersistenceFailure | UnpackFailure, EdgeReconciliationSnapshot]

object EdgeReconciliationSnapshot:

  /** All pairs of half-edges were determined to be reconciled for this window */
  def reconciled(windowStart: Long, windowSize: Long, now: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(
      windowStart = windowStart,
      windowSize = windowSize,
      whenWritten = now,
      state = EdgeReconciliationState.Reconciled
    )

  /** The window is known or suspected of being inconsistent */
  def inconsistent(windowStart: Long, windowSize: Long, now: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(
      windowStart = windowStart,
      windowSize = windowSize,
      whenWritten = now,
      state = EdgeReconciliationState.Inconsistent
    )

  /** The state is unknown. This is not typically something that we expect to write, and it would normally be
    * represented as a gap in the table.
    */
  def unknown(windowStart: Long, windowSize: Long, now: Long): EdgeReconciliationSnapshot =
    EdgeReconciliationSnapshot(
      windowStart = windowStart,
      windowSize = windowSize,
      whenWritten = now,
      state = EdgeReconciliationState.Unknown
    )
