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

object APTDetectionOriginalQuery:

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
          Event.EdgeAdded(Edge("EVENT", eventId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = eventId,
        events = Vector(
          Event.PropertyAdded("type", event_type),
          Event.PropertyAdded("EndpointEvent", ()), // Label not used in predicates or output, but useful for debugging
          Event.PropertyAdded("time", time),
          Event.EdgeAdded(Edge("EVENT", objectId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = objectId,
        events = Vector(
          `object` match { // Data property is only used in output - not in predicates
            case x: String => Event.PropertyAdded("data", x)
            case x: Int    => Event.PropertyAdded("data", x.toLong)
          }
        )
      )
    )

object Endpoint:

  // Automatic derivation doesn't work yet for union types
  implicit val objectDecoder: JsonDecoder[String | Int] = JsonDecoder.peekChar[String | Int] {
    case '"' => JsonDecoder[String].widen
    case _   => JsonDecoder[Int].widen
  }

  implicit val decoder: JsonDecoder[Endpoint] = DeriveJsonDecoder.gen[Endpoint]

object APTDetectionOriginalApp extends ZIOAppDefault:
  // Currently using only the first 1000 lines of the original file to limit test runtime
  val filePrefix = "src/test/scala/andy42/graph/sample/aptdetection"
  //  val endpointPath = s"$filePrefix/endpoint-first-1000.json"
  val endpointPath = s"$filePrefix/endpoint.json"

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
    Ingest(endpointPath, APTDetectionOriginalQuery.subgraphSpec, Endpoint.decoder)


case class Network(
    src_ip: String, // IPv4
    src_port: Int, // uint16
    dst_ip: String, // IPv4
    dst_port: Int, // uint16
    proto: String, // enum
    detail: String | Null, // nullable
    time: Long, // epoch millis presumably
    sequence: String // appears to be integer
) extends IngestableJson:
  override def eventTime: EventTime = time

  override def produceEvents: Vector[NodeMutationInput] =
    val srcId = NodeId.fromNamedValues("Source")(src_ip, src_port)
    val dstId = NodeId.fromNamedValues("Destination")(dst_ip, dst_port)
    val eventId = NodeId.fromNamedProduct("NetTraffic")(this)

    // CREATE (src)-[:NET_TRAFFIC]->(event)-[:NET_TRAFFIC]->(dst)

    Vector(
      NodeMutationInput(
        id = srcId,
        events = Vector(
          Event.PropertyAdded("ip", s"$src_ip:$src_port"),
          Event.PropertyAdded("IP", ()),
          Event.EdgeAdded(Edge("NET_TRAFFIC", eventId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = eventId,
        events = Vector(
          Event.PropertyAdded("proto", proto),
          Event.PropertyAdded("time", time),
          Event.PropertyAdded("detail", detail),
          Event.PropertyAdded("NetTraffic", ()),
          Event.EdgeAdded(Edge("NET_TRAFFIC", dstId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = dstId,
        events = Vector(
          Event.PropertyAdded("ip", s"$dst_ip:$dst_port"),
          Event.PropertyAdded("IP", ())
        )
      )
    )

object Network:
  implicit val decoder: JsonDecoder[Network] = DeriveJsonDecoder.gen[Network]
