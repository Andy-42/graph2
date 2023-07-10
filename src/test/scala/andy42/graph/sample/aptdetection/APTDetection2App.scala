package andy42.graph.sample.aptdetection

import andy42.graph.config.*
import andy42.graph.matcher.*
import andy42.graph.matcher.EdgeSpecs.*
import andy42.graph.model.*
import andy42.graph.persistence.*
import andy42.graph.sample.{Ingest, IngestableJson}
import andy42.graph.services.*
import io.opentelemetry.api.trace.Tracer
import zio.*
import zio.json.*
import zio.logging.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.nio.file.{Files, Paths}

object APTDetection2App extends ZIOAppDefault:

  object StandingQuery:

    val p1 = node("p1").isLabeled("Process")
    val p2 = node("p2").isLabeled("Process")

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

    val ip = node("ip").hasProperty("data")

    val subgraphSpec = subgraph("APT Detection")(
      directedEdge(from = p1, to = writeEvent).edgeKeyIs("TO:EVENT:WRITE"),
      directedEdge(from = writeEvent, to = f).edgeKeyIs("FROM:EVENT:WRITE"),
      directedEdge(from = p2, to = readEvent).edgeKeyIs("TO:EVENT:READ"),
      directedEdge(from = readEvent, to = f).edgeKeyIs("FROM:EVENT:READ"),
      directedEdge(from = p2, to = deleteEvent).edgeKeyIs("TO:EVENT:DELETE"),
      directedEdge(from = deleteEvent, to = f).edgeKeyIs("FROM:EVENT:DELETE"),
      directedEdge(from = p2, to = sendEvent).edgeKeyIs("TO:EVENT:SEND"),
      directedEdge(from = sendEvent, to = ip).edgeKeyIs("FROM:EVENT:SEND")
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

  case class Endpoint(
                       pid: Int,
                       event_type: String, // TODO: This should be encoded as an enum
                       `object`: String | Int,
                       time: Long // epoch millis - this becomes the EventTime for all generated events
                       // sequence: String // appears to be an integer; property is not used
                     ) extends IngestableJson:

    override def eventTime: EventTime = time

    override def produceEvents: Vector[NodeMutationInput] =
      val procId = NodeId.fromNamedValues("Process")(pid)
      val eventId = NodeId.fromNamedProduct("Event")(this)
      val objectId = NodeId.fromNamedValues("Data")(`object`)

      // CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)

      // TODO: Could ignore events with event_type other than: READ | WRITE | DELETE | SEND (e.g., SPAWN, RECEIVE)

      Vector(
        NodeMutationInput(
          id = procId,
          events = Vector(
            Event.PropertyAdded("id", pid.toLong), // Presumably would be of interest for output
            Event.PropertyAdded("Process", ()), // Label is not used in predicates or output, but useful for debugging
            Event.EdgeAdded(Edge(s"TO:EVENT:$event_type", eventId, EdgeDirection.Outgoing))
          )
        ),
        NodeMutationInput(
          id = eventId,
          events = Vector(
            Event.PropertyAdded("type", event_type),
            Event.PropertyAdded("EndpointEvent", ()), // Label not used in predicates or output, but useful for debugging
            Event.PropertyAdded("time", time),
            Event.EdgeAdded(Edge(s"FROM:EVENT:$event_type", objectId, EdgeDirection.Outgoing))
          )
        ),
        NodeMutationInput(
          id = objectId,
          events = Vector(
            `object` match { // Data property is only used in output - not in predicates
              case x: String => Event.PropertyAdded("data", x)
              case x: Int => Event.PropertyAdded("data", x.toLong)
            }
          )
        )
      )

  object Endpoint:

    // Automatic derivation doesn't work yet for union types
    implicit val objectDecoder: JsonDecoder[String | Int] = JsonDecoder.peekChar[String | Int] {
      case '"' => JsonDecoder[String].widen
      case _ => JsonDecoder[Int].widen
    }

    implicit val decoder: JsonDecoder[Endpoint] = DeriveJsonDecoder.gen[Endpoint]

  // Currently using only the first 1000 lines of the original file to limit test runtime
  val filePrefix = "src/test/scala/andy42/graph/sample/aptdetection"
  //  val endpointPath = s"$filePrefix/endpoint-first-1000.json"
  val endpointPath = s"$filePrefix/endpoint.json"

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    Ingest(endpointPath, StandingQuery.subgraphSpec, Endpoint.decoder)

