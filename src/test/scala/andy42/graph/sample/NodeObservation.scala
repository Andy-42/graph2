package andy42.graph.sample

import andy42.graph.model.*
import andy42.graph.services.*
import zio.*
import zio.json.*
import zio.stream.{ZPipeline, ZStream}

import java.nio.file.Paths

trait NodeObservation:
  def eventTime: EventTime
  def produceEvents: Vector[NodeMutationInput]

object NodeObservation:

  def ingestJsonFromFile[T <: NodeObservation](path: String, parallelism: Int)(using
      decoder: JsonDecoder[T]
  ): ZStream[Graph, Throwable | NodeIOFailure | InputValidationFailure, SubgraphMatchAtTime] =
    ZStream
      .fromZIO(ZIO.service[Graph])
      .flatMap { graph =>
        ZStream
          .fromPath(Paths.get(path))
          .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
          .map(_.fromJson[T])
          .collect { case Right(nodeObservation) => nodeObservation } // FIXME: Discarding decode failures - log it!
          .tap(ZIO.debug(_))
          .mapZIOPar(parallelism)(nodeObservation =>
            graph.append(nodeObservation.eventTime, nodeObservation.produceEvents)
          )
          .flatMap(ZStream.fromIterable(_))
      }
