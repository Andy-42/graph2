package andy42.graph.services

import andy42.graph.model.NodeId
import zio.*
import zio.logging.{LogAnnotation, LogFormat}

import java.sql.SQLException

sealed trait PersistenceFailure extends Throwable:
  def message: String
  def id: NodeId
  def op: String // get | append - TODO: enum

trait SQLFailure extends PersistenceFailure:
  override val message = "SQL failure"
  def ex: SQLException

final case class SQLReadFailure(id: NodeId, ex: SQLException) extends SQLFailure:
  override val op = "get"

final case class SQLWriteFailure(id: NodeId, ex: SQLException) extends SQLFailure:
  override val op = "append"

final case class RocksDBiteratorFailure(id: NodeId, ex: Throwable) extends PersistenceFailure:
  override def message: String = ex.getMessage
  override val op = "get"

final case class RocksDBPutFailure(id: NodeId, ex: Throwable) extends PersistenceFailure:
  override def message: String = ex.getMessage
  override val op = "append"


final case class CountPersistenceFailure(id: NodeId, expected: Long, was: Long) extends PersistenceFailure:
  override val op = "append"

  override val message = s"Expected $expected row(s) to be affected by $op, but was $was."
