package andy42.graph.services

import andy42.graph.model.{EdgeHash, Event, EventTime, NodeId}
import andy42.graph.services.{MillisecondDuration, WindowStart}
import zio.logging.LogAnnotation

object LogAnnotations:

  // Edge Reconciliation
  val expiryThresholdAnnotation: LogAnnotation[WindowStart] =
    LogAnnotation[WindowStart]("expiryThreshold", (_, x) => x, _.toString)
  val windowSizeAnnotation: LogAnnotation[MillisecondDuration] =
    LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
  val eventTimeAnnotation: LogAnnotation[EventTime] = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)
  val eventWindowTimeAnnotation: LogAnnotation[WindowStart] =
    LogAnnotation[WindowStart]("eventWindowTime", (_, x) => x, _.toString)
  val windowStartAnnotation: LogAnnotation[WindowStart] =
    LogAnnotation[WindowStart]("windowStart", (_, x) => x, _.toString)
  val edgeHashAnnotation: LogAnnotation[EdgeHash] = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)

  val operationAnnotation: LogAnnotation[String] = LogAnnotation[String]("operation", (_, x) => x, _.toString)

  val nearNodeIdAnnotation: LogAnnotation[NodeId] = LogAnnotation[NodeId]("nearNodeId", (_, x) => x, _.toString)
  val farNodeIdsAnnotation: LogAnnotation[Vector[NodeId]] = 
    LogAnnotation[Vector[NodeId]]("farNodeIds", (a, b) => a ++ b, _.map(_.toString).mkString(","))
  val timeAnnotation: LogAnnotation[EventTime] = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)

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
