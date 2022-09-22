package andy42.graph.services

import zio.logging.LogAnnotation
import andy42.graph.model.{EventTime, EdgeHash, NodeId, Event}

object LogAnnotations:

    def formatNodeId(id: NodeId): String = id.map(_.toInt.toHexString).mkString
    def formatEvent(event: Event): String = event.getClass.getSimpleName

    // Edge Reconciliation
    val expiryThresholdAnnotation = LogAnnotation[WindowStart]("expiryThreshold", (_, x) => x, _.toString)
    val windowSizeAnnotation = LogAnnotation[MillisecondDuration]("windowSize", (_, x) => x, _.toString)
    val eventTimeAnnotation = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)
    val eventWindowTimeAnnotation = LogAnnotation[WindowStart]("eventWindowTime", (_, x) => x, _.toString)
    val windowStartAnnotation = LogAnnotation[WindowStart]("windowStart", (_, x) => x, _.toString)
    val edgeHashAnnotation = LogAnnotation[EdgeHash]("edgeHash", (_, x) => x, _.toString)

    val operationAnnotation = LogAnnotation[String]("operation", (_, x) => x, _.toString)

    val nearNodeIdAnnotation = LogAnnotation[NodeId]("nearNodeId", (_, x) => x, formatNodeId)
    val farNodeIdAnnotation = LogAnnotation[NodeId]("farNodeId", (_, x) => x, formatNodeId)
    val eventAnnotation = LogAnnotation[Event]("event", (_, x) => x, formatEvent)
    val timeAnnotation = LogAnnotation[EventTime]("time", (_, x) => x, _.toString)

    // NodeCache
    val cacheItemRetainWatermark = LogAnnotation[AccessTime]("cacheItemRetainWatermark", (_, x) => x, _.toString)
    val snapshotRetainWatermark = LogAnnotation[AccessTime]("snapshotRetainWatermark", (_, x) => x, _.toString)
