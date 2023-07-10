package andy42.graph.sample

import andy42.graph.model.*
import andy42.graph.persistence.PersistenceFailure
import andy42.graph.services.*
import zio.*
import zio.json.*
import zio.stream.{ZPipeline, ZStream}

import java.nio.file.Paths

trait IngestableJson:
  def eventTime: EventTime
  def produceEvents: Vector[NodeMutationInput]

object IngestableJson:

  def ingestFromFile[T <: IngestableJson](path: String)(parallelism: Int)(using
      decoder: JsonDecoder[T]
  ): ZIO[Graph, Throwable | UnpackFailure | PersistenceFailure, Unit] =
    for
      graph <- ZIO.service[Graph]
      _ <- ZStream
        .fromPath(Paths.get(path))
        .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
        .map(_.fromJson[T])
        .collect { case Right(endpoint) => endpoint } // FIXME: Discarding endpoint decode failures
        .tap(ZIO.debug(_))
        .mapZIOPar(parallelism)(endpoint => graph.append(endpoint.eventTime, endpoint.produceEvents))
        .runDrain
    yield ()
