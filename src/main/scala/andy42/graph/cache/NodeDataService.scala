package andy42.graph.cache

import andy42.graph.model._
import io.getquill._
import io.getquill.context.qzio.ImplicitSyntax._
import zio._
import org.msgpack.core.MessageBufferPacker
import org.msgpack.core.MessagePack
import org.msgpack.core.MessagePacker

import javax.sql.DataSource

trait NodeDataService:

  /** Get the full history of the node. The result will be in ascending order of (time, sequence).
    */
  def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node]

  /** Append an EventsAtTime to a node's persisted history. Within the historyfor a Node, the (time, sequence) must be
    * unique. Within a time, the sequence numbers should be a dense sequence starting at zero.
    */
  def append(
      id: NodeId,
      eventsAtTime: EventsAtTime
  ): IO[PersistenceFailure, Unit]

final case class GraphHistory(
    id: NodeId, // clustering key
    time: EventTime, // sort key
    sequence: Int, // sort key
    events: Array[Byte] // packed payload
) extends Packable:
  def toEventsAtTime: IO[UnpackFailure, EventsAtTime] =
    for events <- Events.unpack(events)
    yield EventsAtTime(time, sequence, events)

   /**
    * Pack a GraphHistory to packed form.
    * This is the same representation as for an EventsAtTime, but
    * packing directly from a GraphHistory avoids having to unpack and repack events.
   */ 
  override def pack(using packer: MessagePacker): Unit =
    packer.packLong(time)
    packer.packInt(sequence)
    packer.writePayload(events)

// TODO: GraphHistory (for single element) should have a different name
object GraphHistory extends UncountedSeqPacker[GraphHistory]:

  def toGraphHistory(id: NodeId, eventsAtTime: EventsAtTime): GraphHistory =
    GraphHistory(
      id = id,
      time = eventsAtTime.time,
      sequence = eventsAtTime.sequence,
      events = eventsAtTime.toPacked
    )

final case class NodeDataServiceLive(ds: DataSource) extends NodeDataService:

  val ctx = PostgresZioJdbcContext(Literal)
  import ctx._

  inline def graph = quote { query[GraphHistory] }

  inline def quotedGet(id: NodeId) = quote {
    graph
      .filter(_.id == lift(id))
      .sortBy(graphHistory => (graphHistory.time, graphHistory.sequence))(
        Ord(Ord.asc, Ord.asc)
      )
  }

  inline def quotedAppend(graphHistory: GraphHistory) = quote {
    graph
      .insertValue(lift(graphHistory))
      .onConflictUpdate(_.id, _.time, _.sequence)((table, excluded) => table.events -> excluded.events)
  }

  given Implicit[DataSource] = Implicit(ds)

  override def get(id: NodeId): IO[PersistenceFailure | UnpackFailure, Node] =
    run(quotedGet(id)).implicitly
      .mapError(SQLReadFailure(id, _))
      .flatMap (history => 
        for nodeHistory <- ZIO.foreach(history.toVector)(_.toEventsAtTime)
        yield 
          if nodeHistory.isEmpty then
            Node.empty(id)
          else
            Node.replaceWithHistory(
              id = id,
              history = nodeHistory,
              packed = GraphHistory.toPacked(history) // Repack history without repacking events
            )
      )

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    run(quotedAppend(GraphHistory.toGraphHistory(id, eventsAtTime))).implicitly
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
