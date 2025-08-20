package andy42.graph.persistence

import andy42.graph.model.*
import org.rocksdb.*
import zio.*
import zio.stream.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

case class RocksDBNodeRepository(db: RocksDB, cfHandle: ColumnFamilyHandle) extends NodeRepository:
  import RocksDBNodeRepository.*

  private case class UnpackedHistory(
      time: EventTime,
      sequence: Int,
      packedEvents: Array[Byte],
      unpackedEvents: Vector[Event]
  )

  private def prefixMatchStream(keyPrefix: Array[Byte]): UStream[(Array[Byte], Array[Byte])] =
    kvStream(acquireKeyPrefixIterator(keyPrefix))

  private def columnFamilyContentsStream: UStream[(Array[Byte], Array[Byte])] =
    kvStream(acquireIterator)

  private def kvStream(acquire: ZIO[Any, Nothing, RocksIterator]): UStream[(Array[Byte], Array[Byte])] =
    for
      it <- ZStream.acquireReleaseWith(acquire)(releaseIterator)
      kv <- drainIterator(it)
    yield kv

  private val readOptions = new ReadOptions().setPrefixSameAsStart(true)

  private def acquireKeyPrefixIterator(keyPrefix: Array[Byte]): UIO[RocksIterator] =
    ZIO.attemptBlocking {
      val it = db.newIterator(cfHandle, readOptions)
      it.seek(keyPrefix)
      it
    }.orDie

  private def acquireIterator: UIO[RocksIterator] = ZIO.attemptBlocking { db.newIterator(cfHandle) }.orDie

  private def releaseIterator(it: RocksIterator): UIO[Unit] = ZIO.succeed(it.close())

  private def drainIterator(it: RocksIterator): UStream[(Array[Byte], Array[Byte])] =
    ZStream.repeatZIOOption {
      ZIO.blocking {
        if it.isValid then
          val next = it.key() -> it.value()
          it.next()
          ZIO.succeed(next)
        else ZIO.fail(None)
      }
    }

  override def get(id: NodeId): IO[UnpackFailure, Node] =
    val idBytes = id.toArray

    for
      historyChunk <- prefixMatchStream(idBytes).runCollect
      nodeHistory <- ZIO.foreach(historyChunk) { (key, packedEvents) =>
        require(key.length == keyLength)
        require(key.startsWith(idBytes))

        val time = ByteBuffer.wrap(key, timeOffset, timeLength).getLong
        val sequence = ByteBuffer.wrap(key, sequenceOffset, sequenceLength).getInt
        Events.unpack(packedEvents).map(UnpackedHistory(time, sequence, packedEvents, _))
      }
    yield
      if nodeHistory.isEmpty then Node.empty(id)
      else
        Node.fromHistory(
          id = id,
          history = nodeHistory.toVector.map { unpackedHistory =>
            EventsAtTime(
              time = unpackedHistory.time,
              sequence = unpackedHistory.sequence,
              events = unpackedHistory.unpackedEvents
            )
          },
          packed = nodeHistory.flatMap(_.packedEvents).toArray
        )

  override def append(id: NodeId, eventsAtTime: EventsAtTime): IO[RocksDBPutFailure, Unit] =
    appendNonEmptyEventsAtTime(id, eventsAtTime)
      .unless(eventsAtTime.events.isEmpty)
      .unit

  private def appendNonEmptyEventsAtTime(id: NodeId, eventsAtTime: EventsAtTime): IO[RocksDBPutFailure, Unit] =
    val keyBytesBuffer = ByteBuffer.allocate(keyLength)
    keyBytesBuffer.put(id.toArray)
    keyBytesBuffer.putLong(eventsAtTime.time)
    keyBytesBuffer.putInt(eventsAtTime.sequence)
    val keyBytes = keyBytesBuffer.array

    ZIO
      .attemptBlocking(db.put(cfHandle, keyBytes, Events.pack(eventsAtTime.events)))
      .refineOrDie { case e: RocksDBException => RocksDBPutFailure(id, e) }

  override def contents: Stream[UnpackFailure, NodeRepositoryEntry] =
    columnFamilyContentsStream.mapZIO { (k, v) =>
      val id = NodeId(ByteBuffer.wrap(k, idOffset, idLength).array())
      val time = ByteBuffer.wrap(k, timeOffset, timeLength).getLong
      val sequence = ByteBuffer.wrap(k, sequenceOffset, sequenceLength).getInt
      Events.unpack(v).map(events => NodeRepositoryEntry(id, time, sequence, events))
    }

object RocksDBNodeRepository:

  // key: id, time, sequence
  val idLength: Int = NodeId.byteLength
  val timeLength: Int = java.lang.Long.BYTES
  val sequenceLength: Int = java.lang.Integer.BYTES

  val keyLength: Int = idLength + timeLength + sequenceLength

  val idOffset: Int = 0
  val timeOffset: Int = idLength
  val sequenceOffset: Int = idLength + timeLength

  private val cfDescriptor: ColumnFamilyDescriptor =
    new ColumnFamilyDescriptor(
      "node-repository".getBytes(UTF_8),
      new ColumnFamilyOptions().useCappedPrefixExtractor(idLength)
    )

  val layer: ZLayer[RocksDB, RocksDBException, NodeRepository] =
    ZLayer {
      for
        db <- ZIO.service[RocksDB]
        cfHandle <- ZIO
          .attemptBlocking(db.createColumnFamily(cfDescriptor))
          .refineOrDie { case e: RocksDBException => e }
      yield RocksDBNodeRepository(db, cfHandle)
    }
