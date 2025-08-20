package andy42.graph.sample.aptdetection

import andy42.graph.config.{AppConfig, MatcherConfig, TracingConfig}
import andy42.graph.matcher.*
import andy42.graph.matcher.EdgeSpecs.*
import andy42.graph.model.*
import andy42.graph.persistence.{RocksDBNodeRepository, TemporaryRocksDB}
import andy42.graph.sample.NodeObservation
import andy42.graph.sample.NodeObservation.ingestJsonFromFile
import andy42.graph.services.*
import zio.*
import zio.json.*
import zio.telemetry.opentelemetry.tracing.Tracing

object APTDetectionOriginalSpec:

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
  ).where(new SubgraphPostFilter:
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
  )

object APTDetectionOriginalApp extends APTDetectionApp:

  val config: ULayer[AppConfig] = ZLayer.succeed {
    AppConfig(
      matcher = MatcherConfig(allNodesInMutationGroupMustMatch = false),
      tracing = TracingConfig(enabled = false)
    )
  }

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (for matches <- ingestJsonFromFile[Endpoint](path = endpointPath, parallelism = 8)(using Endpoint.decoder)
        .tap(subgraphMatch => ZIO.debug(s"Match: $subgraphMatch"))
        .runCollect
      // TODO: Deduplicate matches and sort matches into some canonical order
    yield ()).provide(
      config,
      TemporaryRocksDB.layer,
      RocksDBNodeRepository.layer,
      NodeCache.layer,
      ZLayer.succeed(APTDetectionOriginalSpec.subgraphSpec),
      Graph.layer,
      Telemetry.configurableTracingLayer
    )

  case class Endpoint(
      pid: Int,
      event_type: String, // TODO: This should be encoded as an enum
      `object`: String | Int,
      time: Long // epoch millis - this becomes the EventTime for all generated events
      // sequence: String // appears to be an integer; property is not used
  ) extends NodeObservation:

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
            Event.PropertyAdded(
              "EndpointEvent",
              ()
            ), // Label not used in predicates or output, but useful for debugging
            Event.PropertyAdded("time", time),
            Event.EdgeAdded(Edge("EVENT", objectId, EdgeDirection.Outgoing))
          )
        ),
        NodeMutationInput(
          id = objectId,
          events = Vector(
            `object` match // Data property is only used in output - not in predicates
              case x: String => Event.PropertyAdded("data", x)
              case x: Int    => Event.PropertyAdded("data", x.toLong)
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

  // The network classes are included in the original Quine recipe, but they serve no function.

  case class Network(
      src_ip: String, // IPv4
      src_port: Int, // uint16
      dst_ip: String, // IPv4
      dst_port: Int, // uint16
      proto: String, // enum
      detail: String | Null, // nullable
      time: Long, // epoch millis presumably
      sequence: String // appears to be integer
  ) extends NodeObservation:
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
