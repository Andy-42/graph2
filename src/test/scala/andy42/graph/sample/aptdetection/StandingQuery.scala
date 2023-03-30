package andy42.graph.sample.aptdetection

import andy42.graph.matcher.EdgeDirectionPredicates.*
import andy42.graph.matcher.*
import zio.*

import scala.util.hashing.MurmurHash3.stringHash

object StandingQuery extends ZIOAppDefault:

  import andy42.graph.matcher.NodePredicates.*

  val p1 = node("Process 1") {}
  val p2 = node("Process 2") {}

  val f = node("File") {}

  val writeEvent = node("Event 1") {
    hasProperty("type", "WRITE")
    labeled("EndpointEvent")
  }
  val readEvent = node("Event 2") {
    hasProperty("type", "READ")
    labeled("EndpointEvent")
  }
  val deleteEvent = node("Event 3") {
    hasProperty("type", "DELETE")
    labeled("EndpointEvent")
  }
  val sendEvent = node("Event 4") {
    hasProperty("type", "SEND")
    labeled("EndpointEvent")
  }

  val ip = node("Target of a SEND event") {}

  val edgeSpecs = EdgeSpecs(edgeSpecs =
    Vector(
      directedEdge(from = p1, to = writeEvent).edgeKeyIs("EVENT"),
      directedEdge(from = writeEvent, to = f).edgeKeyIs("EVENT"),
      directedEdge(from = p2, to = readEvent).edgeKeyIs("EVENT"),
      directedEdge(from = readEvent, to = f).edgeKeyIs("EVENT"),
      directedEdge(from = p2, to = deleteEvent).edgeKeyIs("EVENT"),
      directedEdge(from = deleteEvent, to = f).edgeKeyIs("EVENT"),
      directedEdge(from = p2, to = sendEvent).edgeKeyIs("EVENT"),
      directedEdge(from = sendEvent, to = ip).edgeKeyIs("EVENT")
    )
  )

  override def run: ZIO[Any, Any, Any] = ZIO.unit
