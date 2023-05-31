package andy42.graph.sample.aptdetection

import andy42.graph.model.*
import andy42.graph.sample.IngestableJson
import andy42.graph.services.*
import io.opentelemetry.api.trace.Tracer
import zio.*
import zio.json.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.nio.file.{Files, Paths}

case class Endpoint(
    pid: Int,
    event_type: String, // TODO: This should be encoded as an enum
    `object`: String | Int,
    time: Long, // epoch millis - this becomes the EventTime for all generated events
    sequence: String // appears to be an integer
) extends IngestableJson:

  override def eventTime: EventTime = time

  override def produceEvents: Vector[NodeMutationInput] =
    val procId = NodeId.fromNamedValues("Process")(pid)
    val eventId = NodeId.fromNamedProduct("Event")(this)
    val objectId = NodeId.fromNamedValues("Data")(`object`)

    // CREATE (proc)-[:EVENT]->(event)-[:EVENT]->(object)

    Vector(
      NodeMutationInput(
        id = procId,
        events = Vector(
          Event.PropertyAdded("id", pid.toLong),
          Event.PropertyAdded("Process", ()), // Unit value is used for a label
          Event.EdgeAdded(Edge("EVENT", eventId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = eventId,
        events = Vector(
          Event.PropertyAdded("type", event_type),
          Event.PropertyAdded("EndpointEvent", ()),
          Event.EdgeAdded(Edge("EVENT", objectId, EdgeDirection.Outgoing))
        )
      ),
      NodeMutationInput(
        id = objectId,
        events = Vector(
          Event.PropertyAdded("time", time),
          `object` match {
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

object IngestSpec extends ZIOAppDefault:

  val appConfigLayer: ULayer[AppConfig] = ZLayer.succeed(AppConfig(tracer = TracerConfig(enabled = true)))
  val trace: TaskLayer[Tracing & Tracer] = appConfigLayer >>> TracingService.live

  val graphLayer: TaskLayer[Graph] =
    (appConfigLayer ++
      TestNodeRepository.layer ++
      TestNodeCache.layer ++
      TestMatchSink.layer ++
      TestEdgeSynchronization.layer ++
      trace) >>> Graph.layer

  given JsonDecoder[Endpoint] = Endpoint.decoder
  given JsonDecoder[Network] = Network.decoder

  // Currently using only the first 1000 lines of the original file to limit test runtime
  val filePrefix = "src/test/scala/andy42/graph/sample/aptdetection"
  val endpointPath = s"$filePrefix/endpoint-first-1000.json"
  //val endpointPath = s"$filePrefix/endpoint.json"
  val networkPath = s"$filePrefix/network-first-1000.json"

  val ingest: ZIO[Graph, Any, Chunk[SubgraphMatchAtTime]] =
    for
      graph <- ZIO.service[Graph]
      _ <- graph.registerStandingQuery(StandingQuery.subgraphSpec)

      graphLive = graph.asInstanceOf[GraphLive]
      testMatchSink = graphLive.matchSink.asInstanceOf[TestMatchSink]

      _ <- IngestableJson.ingestFromFile[Endpoint](endpointPath)(parallelism = 2)
      matches <- testMatchSink.matches
    yield matches

  override def run: ZIO[Any, Any, Any] =
    (
      for
        _ <- ZIO.unit.withParallelism(2)
        matches <- ingest
        _ <- ZIO.debug(matches)
      yield ()
    ).provide(graphLayer)
