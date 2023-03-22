package andy42.graph.sample.aptdetection

import andy42.graph.model.*
import andy42.graph.services.*
import zio.*
import zio.json.*
import zio.stream.{ZPipeline, ZStream}

import java.nio.file.{Files, Paths}

trait IngestableJson:
  def eventTime: EventTime
  def produceEvents: Vector[NodeMutationInput]

object IngestableJson:
  def ingestFromFile[T <: IngestableJson](path: String)(using
      decoder: JsonDecoder[T]
  ): ZIO[Graph, Throwable | UnpackFailure | PersistenceFailure, Unit] =
    for
      graph <- ZIO.service[Graph]
      _ <- ZStream
        .fromPath(Paths.get(path))
        .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
        .map(_.fromJson[T])
        .collect { case Right(endpoint) => endpoint } // FIXME: Discarding endpoint decode failures
        .map(endpoint => graph.append(endpoint.eventTime, endpoint.produceEvents))
        .runDrain
    yield ()
    

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
    detail: String, // nullable
    time: Long, // epoch millis presumably
    sequence: String // appears to be integer
)

object Network:
  implicit val decoder: JsonDecoder[Network] = DeriveJsonDecoder.gen[Network]

object SampleDataDecoder extends ZIOAppDefault:

  override def run: ZIO[Any, Throwable, Unit] =

    val graphLayer: ULayer[Graph] =
      (TestNodeRepository.layer ++
        TestNodeCache.layer ++
        TestStandingQueryEvaluation.layer ++
        TestEdgeSynchronization.layer) >>> Graph.layer

    given JsonDecoder[Endpoint] = Endpoint.decoder
    IngestableJson.ingestFromFile[Endpoint]("sample-persistent-api-threat/endpoint.json").provide(graphLayer)
