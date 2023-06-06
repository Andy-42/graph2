package andy42.graph.persistence

import andy42.graph.services.*
import org.rocksdb.*
import zio.*

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
      .refineOrDie[RocksDBPutEdgeReconciliationFailure] { case e: RocksDBException =>
        RocksDBPutEdgeReconciliationFailure(e)
      }

object RocksDBEdgeReconciliationRepository:

  val windowStartLength: RuntimeFlags = java.lang.Long.BYTES
  val windowSizeLength: RuntimeFlags = java.lang.Long.BYTES

  val keyLength: RuntimeFlags = windowStartLength + windowSizeLength

  private val columnFamilyDescriptor: ColumnFamilyDescriptor =
    new ColumnFamilyDescriptor(
      "edge-reconciliation-repository".getBytes(UTF_8),
      new ColumnFamilyOptions() // .useCappedPrefixExtractor(idLength)
    )

  val layer: ZLayer[RocksDB, RocksDBException, EdgeReconciliationRepository] =
    ZLayer {
      for
        db <- ZIO.service[RocksDB]
        columnFamilyHandle <- ZIO
          .attemptBlocking(db.createColumnFamily(columnFamilyDescriptor))
          .refineOrDie[RocksDBException] { case e: RocksDBException => e }
      yield RocksDBEdgeReconciliationRepositoryLive(db, columnFamilyHandle)
    }
