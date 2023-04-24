package andy42.graph.services

import zio.Chunk
import zio.logging.{LogAnnotation, logContext}
import zio.test.ZTestLogger.LogEntry

object LogOutputExtract:

  extension (entries: Chunk[LogEntry])
    def extract[T](annotation: LogAnnotation[T]): Chunk[Option[String]] =
      entries.map(_.context.get(logContext).flatMap(_.asMap.get(annotation.name)))

  def messages(logOutput: Chunk[LogEntry]): Chunk[String] =
    logOutput.map(_.message())

  def now(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.now)

  def capacity(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.capacity)

  def previousWatermark(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.previousWatermark)

  def nextWatermark(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.nextWatermark)

  def previousSize(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.previousSize)

  def nextSize(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.nextSize)

  def trimCount(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.extract(LogAnnotations.trimCount)
