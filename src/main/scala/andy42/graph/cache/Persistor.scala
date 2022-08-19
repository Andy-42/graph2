package andy42.graph.cache

import andy42.graph.model.EventsAtTime
import andy42.graph.model.NodeId
import andy42.graph.model.Node
import zio.IO
import java.sql.SQLException

sealed trait PersistenceFailure extends Throwable
case class ReadFailure(id: NodeId, ex: SQLException) extends PersistenceFailure
case class WriteFailure(node: Node, ex: SQLException) extends PersistenceFailure

trait Persistor {

  /** Get the full history of the node. The result will be in ascending order of
    * (eventTime, sequence).
    */
  def get(id: NodeId): IO[ReadFailure, Vector[EventsAtTime]]

  /** Append an EventsAtTime to the node's persisted history. Within the history
    * for a Node, the (eventTime, sequence) must be unique. Within an eventTime,
    * the sequence numbers should be a dense sequence starting at zero.
    */
  def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[PersistenceFailure, Unit]
}
