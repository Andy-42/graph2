package andy42.graph.services

import andy42.graph.model.EventTime
import zio.logging.LogAnnotation

object LogAnnotations:

  val operationAnnotation: LogAnnotation[String] = LogAnnotation[String]("operation", (_, x) => x, _.toString)

// NodeCache
  val now: LogAnnotation[EventTime] =
    LogAnnotation[EventTime]("now", (_, x) => x, _.toString)
  val capacity: LogAnnotation[Int] =
    LogAnnotation[Int]("capacity", (_, x) => x, _.toString)
  val nextWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("nextWatermark", (_, x) => x, _.toString)
  val previousWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("previousWatermark", (_, x) => x, _.toString)
  val previousSize: LogAnnotation[Int] =
    LogAnnotation[Int]("previousSize", (_, x) => x, _.toString)
  val nextSize: LogAnnotation[Int] =
    LogAnnotation[Int]("nextSize", (_, x) => x, _.toString)
  val trimCount: LogAnnotation[Int] =
    LogAnnotation[Int]("trimCount", (_, x) => x, _.toString)
