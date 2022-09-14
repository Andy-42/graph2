package andy42.graph.cache

import andy42.graph.model._
import io.getquill._
import io.getquill.context.qzio.ImplicitSyntax._
import org.msgpack.core._
import zio._

import javax.sql.DataSource

trait NodeDataService:

  /** Get a Node from persistent store.
    *
    * This always gets the full history of a node. If the node is not in the data store, an empty node is returned.
    */
  def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node]

  /** Append an EventsAtTime to a node's persisted history. Within the history for a Node, the (time, sequence) must be
    * unique. Within a time, the sequence numbers should be a dense sequence starting at zero.
    */
  def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[PersistenceFailure, Unit]

/** This class models the persistent data store
  *
  * @param id
  * @param time
  * @param sequence
  * @param events
  */
final case class GraphEventsAtTime(
    id: NodeId, // clustering key
    time: EventTime, // sort key
    sequence: Int, // sort key
    events: Array[Byte] // packed payload
) extends Packable:

  def toEventsAtTime: IO[UnpackFailure, EventsAtTime] =
    for events <- Events.unpack(events)
    yield EventsAtTime(time, sequence, events)

  /** Pack this GraphEventsAtTime to packed form. This is the same representation as for an EventsAtTime, but packing
    * directly from a GraphEventsAtTime avoids having to (unpack and) repack events.
    */
  override def pack(using packer: MessagePacker): Unit =
    packer.packLong(time)
    packer.packInt(sequence)
    packer.writePayload(events)

extension (eventsAtTime: EventsAtTime)
  def toGraphEventsAtTime(id: NodeId): GraphEventsAtTime =
    GraphEventsAtTime(
      id = id,
      time = eventsAtTime.time,
      sequence = eventsAtTime.sequence,
      events = eventsAtTime.toPacked
    )

extension (graphHistory: List[GraphEventsAtTime])
  def toPacked: PackedNodeHistory =
    given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
    graphHistory.foreach(_.pack)
    packer.toByteArray()

final case class NodeDataServiceLive(ds: DataSource) extends NodeDataService:

  val ctx = PostgresZioJdbcContext(Literal)
  import ctx._

  inline def graph = quote { query[GraphEventsAtTime] }

  inline def quotedGet(id: NodeId) = quote {
    graph
      .filter(_.id == lift(id))
      .sortBy(graphEventsAtTime => (graphEventsAtTime.time, graphEventsAtTime.sequence))(
        Ord(Ord.asc, Ord.asc)
      )
  }

  inline def quotedAppend(graphEventsAtTime: GraphEventsAtTime) = quote {
    graph
      .insertValue(lift(graphEventsAtTime))
      .onConflictUpdate(_.id, _.time, _.sequence)((table, excluded) => table.events -> excluded.events)
  }

  given Implicit[DataSource] = Implicit(ds)

  override def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    run(quotedGet(id)).implicitly
      .mapError(SQLReadFailure(id, _))
      .flatMap(history =>
        // Unpacking the node history at this point validates that the node unpacks correctly.
        for nodeHistory <- ZIO.foreach(history.toVector)(_.toEventsAtTime)
        yield
          if nodeHistory.isEmpty then Node.empty(id)
          else
            Node.replaceWithHistory(
              id = id,
              history = nodeHistory,
              packed = history.toPacked
            )
      )

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    run(quotedAppend(eventsAtTime.toGraphEventsAtTime(id))).implicitly
      .mapError(SQLWriteFailure(id, _))
      .flatMap { rowsInsertedOrUpdated =>
        if rowsInsertedOrUpdated != 1 then
          ZIO.fail(CountPersistenceFailure(id, expected = 1, was = rowsInsertedOrUpdated))
        else ZIO.unit
      }

object NodeDataService {
  val layer: URLayer[DataSource, NodeDataService] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield NodeDataServiceLive(ds)
    }
}
