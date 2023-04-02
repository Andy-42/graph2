package andy42.graph.sample.aptdetection

import andy42.graph.matcher.*
import andy42.graph.matcher.EdgeSpecs.*
import zio.*

object StandingQuery extends ZIOAppDefault:

  import andy42.graph.matcher.NodePredicates.*

  val p1 = node("Process 1")()
  val p2 = node("Process 2")()

  val f = node("File")()

  val writeEvent = node("Event 1")(hasProperty("type", "WRITE"), isLabeled("EndpointEvent"))
  val readEvent = node("Event 2")(hasProperty("type", "READ"), isLabeled("EndpointEvent"))
  val deleteEvent = node("Event 3")(hasProperty("type", "DELETE"), isLabeled("EndpointEvent"))
  val sendEvent = node("Event 4")(hasProperty("type", "SEND"), isLabeled("EndpointEvent"))

  val ip = node("Target of a SEND event")()

  val subgraphSpec = subgraph(
    directedEdge(from = p1, to = writeEvent).edgeKeyIs("EVENT"),
    directedEdge(from = writeEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = readEvent).edgeKeyIs("EVENT"),
    directedEdge(from = readEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = deleteEvent).edgeKeyIs("EVENT"),
    directedEdge(from = deleteEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = sendEvent).edgeKeyIs("EVENT"),
    directedEdge(from = sendEvent, to = ip).edgeKeyIs("EVENT")
  ) where {
    for
      writeTime <- writeEvent.epochMillis("time")
      readTime <- readEvent.epochMillis("time")
      deleteTime <- readEvent.epochMillis("time")
      sendTime <- sendEvent.epochMillis("time")

    // In the Quine sample, this expression uses '<', which would not match events happening within 1 ms resolution.
    yield writeTime <= readTime && readTime <= deleteTime && deleteTime <= sendTime
  }

  override def run: ZIO[Any, Any, Any] = ZIO.unit
