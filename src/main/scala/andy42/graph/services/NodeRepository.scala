package andy42.graph.services

import andy42.graph.model.*
import org.msgpack.core.*
import zio.*

import javax.sql.DataSource

trait NodeRepository:

  /** Get a Node from persistent store.
    *
    * This always gets the full history of a node. If the node is not in the data store, an empty node is returned.
    */
  def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node]

  /** Append an EventsAtTime to a node's persisted history. Within the history for a Node, the (time, sequence) must be
    * unique. Within a time, the sequence numbers should be a dense sequence starting at zero.
    *
    * The graph model should have perfect knowledge of the state of the persistent store, so an append should never fail
    * due to a duplicate key.
    */
  def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[PersistenceFailure, Unit]
