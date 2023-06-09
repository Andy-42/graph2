package andy42.graph.persistence

import andy42.graph.model.*
import andy42.graph.services.*
import io.getquill.*
import io.getquill.context.qzio.ImplicitSyntax.*
import org.msgpack.core.{MessageBufferPacker, MessagePack, MessagePacker}
import zio.*
import zio.stream.Stream

import javax.sql.DataSource

final case class PostgresNodeRepository(ds: DataSource) extends NodeRepository:

  val ctx: PostgresZioJdbcContext[Literal] = PostgresZioJdbcContext(Literal)
  import ctx.*

  inline def graph: EntityQuery[GraphEventsAtTime] = query[GraphEventsAtTime]

  private inline def quotedGet(id: NodeId): Query[GraphEventsAtTime] =
    graph
      .filter(_.id == lift(id))
      .sortBy(graphEventsAtTime => (graphEventsAtTime.time, graphEventsAtTime.sequence))(
        Ord(Ord.asc, Ord.asc)
      )

  private inline def quotedAppend(graphEventsAtTime: GraphEventsAtTime): Insert[GraphEventsAtTime] =
    graph.insertValue(lift(graphEventsAtTime))

  given Implicit[DataSource] = Implicit(ds)

  given MappedEncoding[NodeId, Array[Byte]](_.toArray)
  given MappedEncoding[Array[Byte], NodeId](NodeId(_))

  // FIXME: Why did I have to copy this from NodeRepository?
  import org.msgpack.core.*
  extension (graphHistory: List[GraphEventsAtTime])
    def toPacked: PackedNodeHistory =
      given packer: MessageBufferPacker = MessagePack.newDefaultBufferPacker()
      graphHistory.foreach(_.pack)
      packer.toByteArray

  override def get(id: NodeId): NodeIO[Node] =
    run(quotedGet(id)).implicitly
      .mapError(SQLNodeGetFailure(id, _))
      .flatMap(history =>
        // Unpacking the NodeHistory is not strictly necessary at this point, but it is done because:
        //  - it validates that the node unpacks correctly as it enters the system from the data store, and
        //  - it allows the Node constructor to create a current NodeSnapshot and cache it in the instance.
        for nodeHistory <- ZIO.foreach(history.toVector)(_.toEventsAtTime)
        yield
          if nodeHistory.isEmpty then Node.empty(id)
          else
            Node.fromHistory(
              id = id,
              history = nodeHistory,
              packed = history.toPacked
            )
      )

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    run(quotedAppend(eventsAtTime.toGraphEventsAtTime(id))).implicitly
      .mapError(SQLNodeEntryAppendFailure(id, eventsAtTime.time, eventsAtTime.sequence, _))
      .foldZIO(
        failure = ZIO.fail,
        success =
          rowsAffected => ZIO.fail(NodeAppendCountPersistenceFailure(id, rowsAffected)).unless(rowsAffected == 1)
      )
      .unit

  override def contents: Stream[PersistenceFailure | UnpackFailure, NodeRepositoryEntry] = ???

object PostgresNodeRepository:
  val layer: URLayer[DataSource, NodeRepository] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield PostgresNodeRepository(ds)
    }

/** GraphEventsAtTime models the persistent data store.
  *
  * TODO: Define an ordering that should be the same as the repository ordering
  *
  * @param id
  *   The Node identifier; clustering key.
  * @param time
  *   The epoch millis time when the events were appended to in the history; sort key.
  * @param sequence
  *   A disambiguator if multiple appends happen to the same event and time; sort key.
  * @param events
  *   The events written as a counted sequence packed Event.
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
    packer.toByteArray
