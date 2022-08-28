package andy42.graph.cache

import java.sql.SQLException

import andy42.graph.model.NodeId

sealed trait PersistenceFailure extends Throwable {
  def id: NodeId
}
trait ReadFailure extends PersistenceFailure
trait WriteFailure extends PersistenceFailure

trait SQLFailure {
  def ex: SQLException
}

case class SQLReadFailure(id: NodeId, ex: SQLException)
    extends ReadFailure
    with SQLFailure

case class SQLWriteFailure(id: NodeId, ex: SQLException)
    extends WriteFailure
    with SQLFailure

trait CountAffectedFailure {
  def expected: Long
  def was: Long
}

case class CountPersistenceFailure(id: NodeId, expected: Long, was: Long)
    extends WriteFailure
    with CountAffectedFailure