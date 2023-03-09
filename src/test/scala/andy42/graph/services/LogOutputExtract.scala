package andy42.graph.services

import zio.Chunk
import zio.logging.logContext
import zio.test.ZTestLogger.LogEntry

object LogOutputExtract:

  def messages(logOutput: Chunk[LogEntry]): Chunk[String] =
    logOutput.map(_.message())

  def nowAnnotations(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.map(_.context.get(logContext).flatMap(_.asMap.get(LogAnnotations.now.name)))

  def cacheItemRetainWatermark(logOutput: Chunk[LogEntry]): Chunk[Option[String]] =
    logOutput.map(_.context.get(logContext).flatMap(_.asMap.get(LogAnnotations.cacheItemRetainWatermark.name)))
