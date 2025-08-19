package andy42.graph.persistence

import org.apache.commons.io.FileUtils
import org.rocksdb.{Options, RocksDB, RocksDBException}
import zio.*

import java.io.IOException
import java.nio.file.{Files, Path}

object TemporaryRocksDB:

  val layer: Layer[IOException | RocksDBException, RocksDB] =
    ZLayer.scoped {
      for
        dir <- managedPath(s"graph-temp-rocksdb")
        options = new org.rocksdb.Options().setCreateIfMissing(true)
        db <- managedDb(options, dir)
      yield db
    }

  def managedPath(directory: String): ZIO[Scope, IOException, Path] =
    ZIO.acquireRelease(createTempDirectory(directory))(deleteDirectory)

  def createTempDirectory(prefix: String): ZIO[Any, IOException, Path] =
    ZIO.attemptBlockingIO(Files.createTempDirectory(prefix))

  def deleteDirectory(directory: Path): UIO[Unit] =
    ZIO.attemptBlockingIO(FileUtils.deleteDirectory(directory.toFile)).orElseSucceed(()) // TODO: Log failed delete

  def managedDb(options: org.rocksdb.Options, directory: Path): ZIO[Scope, RocksDBException, RocksDB] =
    ZIO.acquireRelease(openRocksDB(options, directory))(closeRocksDB)

  def openRocksDB(options: org.rocksdb.Options, directory: Path): ZIO[Any, RocksDBException, RocksDB] =
    ZIO
      .attemptBlocking(RocksDB.open(options, directory.toAbsolutePath.toString))
      .refineOrDie { case e: RocksDBException => e }

  def closeRocksDB(db: RocksDB): ZIO[Any, Nothing, Unit] =
    ZIO.attemptBlocking(db.close()).orElseSucceed(()) // TODO: Log db.close() failure, use closeE
