package andy42.graph.cache

import java.sql.SQLException

import andy42.graph.model.NodeId

import zio.logging.{LogAnnotation, LogFormat }
import zio._

sealed trait PersistenceFailure extends Throwable:
  def message: String
  def id: NodeId
  def op: String

trait SQLFailure extends PersistenceFailure:
  override val message = "SQL failure"
  def ex: SQLException

final case class SQLReadFailure(id: NodeId, ex: SQLException) extends SQLFailure:
  override val op = "read"

final case class SQLWriteFailure(id: NodeId, ex: SQLException) extends SQLFailure:
  override val op = "append"

final case class CountPersistenceFailure(id: NodeId, expected: Long, was: Long) extends PersistenceFailure:
  override val op = "append"

  override val message = s"Expected $expected row(s) to be affected by $op, but was $was."
