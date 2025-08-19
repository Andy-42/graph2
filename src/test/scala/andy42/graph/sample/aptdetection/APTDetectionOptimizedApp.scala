package andy42.graph.sample.aptdetection

import andy42.graph.config.{AppConfig, MatcherConfig, TracerConfig}
import andy42.graph.matcher.*
import andy42.graph.matcher.EdgeSpecs.*
import andy42.graph.model.*
import andy42.graph.persistence.{NodeRepository, RocksDBNodeRepository, TemporaryRocksDB}
import andy42.graph.sample.NodeObservation
import andy42.graph.sample.NodeObservation.ingestJsonFromFile
import andy42.graph.services.*
import org.rocksdb.RocksDB
import zio.*
import zio.json.*
import zio.telemetry.opentelemetry.tracing.Tracing

object APTDetectionOptimizedSpec:

  val p1: NodeSpec = node("p1").isLabeled("Process")
  val p2: NodeSpec = node("p2").isLabeled("Process")

  val f: NodeSpec = node("f")

  val writeEvent: NodeSpec = node("e1")
    .hasProperty("type", "WRITE")
    .isLabeled("EndpointEvent")
  val readEvent: NodeSpec = node("e2")
    .hasProperty("type", "READ")
    .isLabeled("EndpointEvent")
  val deleteEvent: NodeSpec = node("e3")
    .hasProperty("type", "DELETE")
    .isLabeled("EndpointEvent")
  val sendEvent: NodeSpec = node("e4")
    .hasProperty("type", "SEND")
    .isLabeled("EndpointEvent")

  val ip: NodeSpec = node("ip").hasProperty("data")

  val subgraphSpec: SubgraphSpec = subgraph("APT Detection")(
    directedEdge(from = p1, to = writeEvent).edgeKeyIs("TO:EVENT:WRITE"),
    directedEdge(from = writeEvent, to = f).edgeKeyIs("FROM:EVENT:WRITE"),
    directedEdge(from = p2, to = readEvent).edgeKeyIs("TO:EVENT:READ"),
    directedEdge(from = readEvent, to = f).edgeKeyIs("FROM:EVENT:READ"),
    directedEdge(from = p2, to = deleteEvent).edgeKeyIs("TO:EVENT:DELETE"),
    directedEdge(from = deleteEvent, to = f).edgeKeyIs("FROM:EVENT:DELETE"),
    directedEdge(from = p2, to = sendEvent).edgeKeyIs("TO:EVENT:SEND"),
    directedEdge(from = sendEvent, to = ip).edgeKeyIs("FROM:EVENT:SEND")
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

object APTDetectionOptimizedApp extends APTDetectionApp:

  val config: ULayer[AppConfig] = ZLayer.succeed {
    AppConfig(
      matcher = MatcherConfig(allNodesInMutationGroupMustMatch = true),
      tracer = TracerConfig(enabled = true)
    )
  }

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (for
      config <- ZIO.service[AppConfig]
      matches <- ingestJsonFromFile[Endpoint](path = endpointPath, parallelism = 8)(using Endpoint.decoder)
        .tap(subgraphMatch => ZIO.debug(s"Match: $subgraphMatch"))
        .runCollect
    // TODO: Deduplicate matches and sort matches into some canonical order
    yield ()).provide(
      config,
      TemporaryRocksDB.layer,
      RocksDBNodeRepository.layer,
      NodeCache.layer,
      ZLayer.succeed(APTDetectionOptimizedSpec.subgraphSpec),
      Graph.layer,
      andy42.graph.services.OpenTelemetry.configurableTracerLayer,
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
      if Endpoint.eventTypesOfInterest.contains(this.event_type) then
        // Ignore events with event_type other than: READ | WRITE | DELETE | SEND (e.g., SPAWN, RECEIVE)
        Vector.empty
      else
        // CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)
        val procId = NodeId.fromNamedValues("Process")(pid)
        val eventId = NodeId.fromNamedProduct("Event")(this)
        val objectId = NodeId.fromNamedValues("Data")(`object`)

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
              Event.PropertyAdded(
                "EndpointEvent",
                ()
              ), // Label not used in predicates or output, but useful for debugging
              Event.PropertyAdded("time", time),
              Event.EdgeAdded(Edge(s"FROM:EVENT:$event_type", objectId, EdgeDirection.Outgoing))
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

    val eventTypesOfInterest: Set[NodeSpecName] = Set("READ", "WRITE", "DELETE", "SEND")

    // Automatic derivation doesn't work yet for union types
    implicit val objectDecoder: JsonDecoder[String | Int] = JsonDecoder.peekChar[String | Int] {
      case '"' => JsonDecoder[String].widen
      case _   => JsonDecoder[Int].widen
    }

    implicit val decoder: JsonDecoder[Endpoint] = DeriveJsonDecoder.gen[Endpoint]
