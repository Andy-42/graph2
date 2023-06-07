package andy42.graph.persistence

import andy42.graph.model.{EventTime, NodeId}
import org.rocksdb.RocksDBException
import zio.*
import zio.logging.{LogAnnotation, LogFormat}

import java.sql.SQLException

sealed trait PersistenceFailure:
  def message: String

trait SQLFailure extends PersistenceFailure:
  val ex: SQLException
  override def message: String = ex.getMessage

final case class SQLNodeGetFailure(id: NodeId, ex: SQLException) extends SQLFailure

final case class SQLEdgeReconciliationContentsFailure(ex: SQLException) extends SQLFailure

final case class SQLNodeEntryAppendFailure(id: NodeId, time: EventTime, sequence: Int, ex: SQLException)
    extends SQLFailure

final case class SQLEdgeReconciliationMarkWindowFailure(
    windowStart: EventTime,
    windowSize: Long,
    state: Byte,
    ex: SQLException
) extends SQLFailure

final case class CountPersistenceFailure(id: NodeId, expected: Long, was: Long) extends PersistenceFailure:
  override val message = s"Expected $expected row(s) to be affected by write, but was $was."

trait RocksDBFailure extends PersistenceFailure:
  val ex: RocksDBException
  override def message: String = ex.getMessage
  
// Note that RockDB does not throw any exceptions on reading  

final case class RocksDBPutFailure(id: NodeId, ex: RocksDBException) extends RocksDBFailure

final case class RocksEdgeReconciliationMarkWindowFailure(
    windowStart: EventTime,
    windowSize: Long,
    state: Byte,
    ex: RocksDBException
) extends RocksDBFailure
