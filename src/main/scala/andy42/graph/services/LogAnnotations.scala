package andy42.graph.services

import zio.logging.LogAnnotation
import andy42.graph.model.{EventTime, EdgeHash, NodeId, Event}

object LogAnnotations:

  def formatNodeId(id: NodeId): String = id.map(_.toInt.toHexString).mkString
  def formatEvent(event: Event): String = event.getClass.getSimpleName

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

  val operationAnnotation: LogAnnotation[String] = LogAnnotation[String]("operation", (_, x) => x, _)

  val nearNodeIdAnnotation: LogAnnotation[NodeId] = LogAnnotation[NodeId]("nearNodeId", (_, x) => x, formatNodeId)
  val farNodeIdAnnotation: LogAnnotation[NodeId] = LogAnnotation[NodeId]("farNodeId", (_, x) => x, formatNodeId)
  val eventAnnotation: LogAnnotation[Event] = LogAnnotation[Event]("event", (_, x) => x, formatEvent)
  val timeAnnotation: LogAnnotation[EventTime] = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)

  // NodeCache
  val cacheItemRetainWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("cacheItemRetainWatermark", (_, x) => x, _.toString)
  val snapshotRetainWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("snapshotRetainWatermark", (_, x) => x, _.toString)
