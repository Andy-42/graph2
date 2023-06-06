package andy42.graph.persistence

import andy42.graph.model.NodeId
import org.rocksdb.RocksDBException
import zio.*
import zio.logging.{LogAnnotation, LogFormat}

import java.sql.SQLException

sealed trait PersistenceFailure:
  def message: String

trait SQLFailure extends PersistenceFailure:
  val ex: SQLException
  override def message: String = ex.getMessage

final case class SQLReadFailure(id: NodeId, ex: SQLException) extends SQLFailure

final case class SQLWriteFailure(id: NodeId, ex: SQLException) extends SQLFailure

final case class CountPersistenceFailure(id: NodeId, expected: Long, was: Long) extends PersistenceFailure:
  override val message = s"Expected $expected row(s) to be affected by write, but was $was."

trait RocksDBFailure extends PersistenceFailure:
  val ex: RocksDBException
  override def message: String = ex.getMessage

final case class RocksDBIteratorFailure(id: NodeId, ex: RocksDBException) extends RocksDBFailure

final case class RocksDBPutFailure(id: NodeId, ex: RocksDBException) extends RocksDBFailure

final case class RocksDBPutEdgeReconciliationFailure(ex: RocksDBException) extends RocksDBFailure
