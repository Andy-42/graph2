package andy42.graph.persistence

import andy42.graph.model.*
import andy42.graph.services.*
import io.getquill.*
import io.getquill.context.qzio.ImplicitSyntax.*
import org.msgpack.core.{MessageBufferPacker, MessagePack, MessagePacker}
import zio.*
import zio.stream.Stream

import java.sql.SQLException
import javax.sql.DataSource

final case class PostgresNodeRepository(ds: DataSource) extends NodeRepository:

  val ctx: PostgresZioJdbcContext[Literal] = PostgresZioJdbcContext(Literal)
  import ctx.*

  inline def graph: EntityQuery[NodeRepositoryEntryWithPackedEvents] = query[NodeRepositoryEntryWithPackedEvents]

  private inline def quotedGet(id: NodeId): Query[NodeRepositoryEntryWithPackedEvents] =
    graph
      .filter(_.id == lift(id))
      .sortBy(row => (row.time, row.sequence))(
        Ord(Ord.asc, Ord.asc)
      )

  private inline def quotedAppend(
      entryWithPackedEvents: NodeRepositoryEntryWithPackedEvents
  ): Insert[NodeRepositoryEntryWithPackedEvents] =
    graph.insertValue(lift(entryWithPackedEvents))

  private inline def contentsQuery: Query[NodeRepositoryEntryWithPackedEvents] =
    graph
      .sortBy(row => (row.time, row.sequence))(
        Ord(Ord.asc, Ord.asc)
      )

  given Implicit[DataSource] = Implicit(ds)

  given MappedEncoding[NodeId, Array[Byte]](_.toArray)
  given MappedEncoding[Array[Byte], NodeId](NodeId(_))

  override def get(id: NodeId): NodeIO[Node] =
    for
      historyWithPackedEvents <- run(quotedGet(id)).implicitly
        .refineOrDie { case e: SQLException => SQLNodeGetFailure(id, e) }
      historyWithPackedEventsArray = historyWithPackedEvents.toArray
      nodeHistory <- ZIO.foreach(historyWithPackedEventsArray)(_.unpackToEventsAtTime)
    yield
      if historyWithPackedEvents.isEmpty then Node.empty(id)
      else
        Node.fromHistory(
          id = id,
          history = nodeHistory.toVector,
          packed = historyWithPackedEventsArray.flatMap(_.packedEvents)
        )

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[PersistenceFailure, Unit] =
    run(quotedAppend(eventsAtTime.toNodeRepositoryEntryWithPackedEvents(id))).implicitly
      .refineOrDie { case e: SQLException =>
        SQLNodeEntryAppendFailure(id, eventsAtTime.time, eventsAtTime.sequence, e)
      }
      .foldZIO(
        failure = ZIO.fail,
        success =
          rowsAffected => ZIO.fail(NodeAppendCountPersistenceFailure(id, rowsAffected)).unless(rowsAffected == 1)
      )
      .unit

  override def contents: Stream[PersistenceFailure | UnpackFailure, NodeRepositoryEntry] =
    stream(contentsQuery).implicitly
      .refineOrDie { case e: SQLException => SQLNodeContentsFailure(e) }
      .mapZIO { nodeRepositoryEntryWithPackedEvents =>
        for events <- Events.unpack(nodeRepositoryEntryWithPackedEvents.packedEvents)
        yield NodeRepositoryEntry(
          id = nodeRepositoryEntryWithPackedEvents.id,
          time = nodeRepositoryEntryWithPackedEvents.time,
          sequence = nodeRepositoryEntryWithPackedEvents.sequence,
          events = events
        )
      }

  case class NodeRepositoryEntryWithPackedEvents(id: NodeId, time: EventTime, sequence: Int, packedEvents: Array[Byte]):
    def unpackToEventsAtTime: IO[UnpackFailure, EventsAtTime] =
      for events <- Events.unpack(packedEvents)
      yield EventsAtTime(time, sequence, events)

  extension (eventsAtTime: EventsAtTime)
    private def toNodeRepositoryEntryWithPackedEvents(id: NodeId): NodeRepositoryEntryWithPackedEvents =
      NodeRepositoryEntryWithPackedEvents(
        id = id,
        time = eventsAtTime.time,
        sequence = eventsAtTime.sequence,
        packedEvents = Events.pack(eventsAtTime.events)
      )

object PostgresNodeRepository:
  val layer: URLayer[DataSource, NodeRepository] =
    ZLayer {
      for ds <- ZIO.service[DataSource]
      yield PostgresNodeRepository(ds)
    }
