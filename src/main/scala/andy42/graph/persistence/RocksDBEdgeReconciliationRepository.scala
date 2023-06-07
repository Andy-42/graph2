package andy42.graph.persistence

import andy42.graph.services.*
import org.rocksdb.*
import zio.*
import zio.stream.{UStream, ZStream}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

case class RocksDBEdgeReconciliationRepositoryLive(db: RocksDB, cfHandle: ColumnFamilyHandle)
    extends EdgeReconciliationRepository:

  import RocksDBEdgeReconciliationRepository.*

  override def markWindow(edgeReconciliation: EdgeReconciliationSnapshot): IO[PersistenceFailure, Unit] =
    val keyBytesBuffer = ByteBuffer.allocate(keyLength)
    keyBytesBuffer.putLong(edgeReconciliation.windowStart)
    keyBytesBuffer.putLong(edgeReconciliation.windowSize)
    val keyBytes = keyBytesBuffer.array()

    val valueBytes: Array[Byte] = Array(edgeReconciliation.state)

    ZIO
      .attemptBlocking(db.put(cfHandle, keyBytes, valueBytes))
      .refineOrDie { case e: RocksDBException =>
        RocksEdgeReconciliationMarkWindowFailure(
          windowStart = edgeReconciliation.windowStart,
          windowSize = edgeReconciliation.windowStart,
          state = edgeReconciliation.state,
          ex = e
        )
      }

  override def contents: UStream[EdgeReconciliationSnapshot] =
    for
      it <- ZStream.acquireReleaseWith(acquireIterator)(releaseIterator)
      edgeReconciliationSnapshot <- drainIterator(it)
    yield edgeReconciliationSnapshot

  private def acquireIterator: UIO[RocksIterator] =
    ZIO.attemptBlocking { db.newIterator(cfHandle) }.orDie

  private def releaseIterator(it: RocksIterator): UIO[Unit] = ZIO.succeed(it.close())

  private def drainIterator(it: RocksIterator): UStream[EdgeReconciliationSnapshot] =
    ZStream.repeatZIOOption {
      ZIO.blocking {
        if it.isValid then

          val keyBuffer = ByteBuffer.wrap(it.key())
          val windowStart = keyBuffer.getLong
          val windowSize = keyBuffer.getLong
          val state = it.value()(0)

          val next = EdgeReconciliationSnapshot(windowStart, windowSize, state)
          it.next()
          ZIO.succeed(next)
        else ZIO.fail(None)
      }
    }

object RocksDBEdgeReconciliationRepository:

  val windowStartLength: RuntimeFlags = java.lang.Long.BYTES
  val windowSizeLength: RuntimeFlags = java.lang.Long.BYTES

  val keyLength: RuntimeFlags = windowStartLength + windowSizeLength

  private val cfDescriptor: ColumnFamilyDescriptor =
    new ColumnFamilyDescriptor(
      "edge-reconciliation-repository".getBytes(UTF_8),
      new ColumnFamilyOptions() // .useCappedPrefixExtractor(idLength)
    )

  val layer: ZLayer[RocksDB, RocksDBException, EdgeReconciliationRepository] =
    ZLayer {
      for
        db <- ZIO.service[RocksDB]
        cfHandle <- ZIO
          .attemptBlocking(db.createColumnFamily(cfDescriptor))
          .refineOrDie { case e: RocksDBException => e }
      yield RocksDBEdgeReconciliationRepositoryLive(db, cfHandle)
    }
