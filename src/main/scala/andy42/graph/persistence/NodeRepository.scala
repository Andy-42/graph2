package andy42.graph.persistence

import andy42.graph.model.*
import zio.*
import zio.stream.Stream

/** NodeRepositoryEntry is one row in the persistent data store.
  *
  * TODO: Define a canonical ordering that should be the same as the repository ordering TODO: Describe how this relates
  * to EventsAtTime and NodeHistory
  *
  * @param id
  *   The Node identifier; clustering key.
  * @param time
  *   The epoch millis time when the events were appended to in the history; sort key.
  * @param sequence
  *   A disambiguator if multiple appends happen to the same event and time; sort key.
  * @param events
  *   The events written as a counted sequence of packed Events.
  */
case class NodeRepositoryEntry(id: NodeId, time: EventTime, sequence: Int, events: Vector[Event])

implicit val nodeRepositoryEntryOrdering: Ordering[NodeRepositoryEntry] =
  Ordering
    .by[NodeRepositoryEntry, NodeId](_.id)
    .orElseBy(_.time)
    .orElseBy(_.sequence)

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

  def contents: Stream[PersistenceFailure | UnpackFailure, NodeRepositoryEntry]
