package andy42.graph.sample.aptdetection

import andy42.graph.matcher.*
import andy42.graph.matcher.EdgeSpecs.*
import andy42.graph.model.NodeIO
import zio.*

object StandingQuery extends App:

  val p1 = node("p1")
  val p2 = node("p2")

  val f = node("f")

  val writeEvent = node("e1")
    .hasProperty("type", "WRITE")
    .isLabeled("EndpointEvent")
  val readEvent = node("e2")
    .hasProperty("type", "READ")
    .isLabeled("EndpointEvent")
  val deleteEvent = node("e3")
    .hasProperty("type", "DELETE")
    .isLabeled("EndpointEvent")
  val sendEvent = node("e4")
    .hasProperty("type", "SEND")
    .isLabeled("EndpointEvent")

  val ip = node("ip")
  
  val subgraphSpec = subgraph("APT Detection")(
    directedEdge(from = p1, to = writeEvent).edgeKeyIs("EVENT"),
    directedEdge(from = writeEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = readEvent).edgeKeyIs("EVENT"),
    directedEdge(from = readEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = deleteEvent).edgeKeyIs("EVENT"),
    directedEdge(from = deleteEvent, to = f).edgeKeyIs("EVENT"),
    directedEdge(from = p2, to = sendEvent).edgeKeyIs("EVENT"),
    directedEdge(from = sendEvent, to = ip).edgeKeyIs("EVENT")
  ) where new SubgraphPostFilter:
    override def description: String = "write.time <= read.time <= delete.time <= sendTime"

    override def p: SnapshotProvider ?=> NodeIO[Boolean] =
      for
        writeTime <- writeEvent.epochMillis("time")
        readTime <- readEvent.epochMillis("time")
        deleteTime <- readEvent.epochMillis("time")
        sendTime <- sendEvent.epochMillis("time")
      // In the Quine APT Detection recipe, this expression uses '<',
      // which would not match events happening within a 1 ms resolution.
      yield writeTime <= readTime && readTime <= deleteTime && deleteTime <= sendTime

  println(subgraphSpec.mermaid)
