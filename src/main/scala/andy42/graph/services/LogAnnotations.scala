package andy42.graph.services

import andy42.graph.model.EdgeHash
import andy42.graph.model.Event
import andy42.graph.model.EventTime
import andy42.graph.model.NodeId
import andy42.graph.services.MillisecondDuration
import andy42.graph.services.WindowStart
import zio.logging.LogAnnotation

object LogAnnotations:

  def formatNodeId(id: NodeId): String = id.toString
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

  val operationAnnotation: LogAnnotation[String] = LogAnnotation[String]("operation", (_, x) => x, _.toString)

  val nearNodeIdAnnotation: LogAnnotation[NodeId] = LogAnnotation[NodeId]("nearNodeId", (_, x) => x, formatNodeId)
  val farNodeIdAnnotation: LogAnnotation[NodeId] = LogAnnotation[NodeId]("farNodeId", (_, x) => x, formatNodeId)
  val eventAnnotation: LogAnnotation[Event] = LogAnnotation[Event]("event", (_, x) => x, formatEvent)
  val timeAnnotation: LogAnnotation[EventTime] = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)

  // NodeCache
  val cacheItemRetainWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("cacheItemRetainWatermark", (_, x) => x, _.toString)
  val snapshotRetainWatermark: LogAnnotation[AccessTime] =
    LogAnnotation[AccessTime]("snapshotRetainWatermark", (_, x) => x, _.toString)
