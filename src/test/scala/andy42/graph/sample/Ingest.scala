package andy42.graph.sample

import andy42.graph.config.*
import andy42.graph.matcher.SubgraphSpec
import andy42.graph.model.*
import andy42.graph.persistence.*
import andy42.graph.sample.IngestableJson
import andy42.graph.services.*
import io.opentelemetry.api.trace.Tracer
import zio.*
import zio.json.*
import zio.logging.*
import zio.stream.{ZPipeline, ZStream}
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.nio.file.{Files, Paths}

object Ingest:

  def ingest[T <: IngestableJson](
      filePath: String,
      subgraphSpec: SubgraphSpec,
      decoder: JsonDecoder[T]
  ): ZIO[Graph, Throwable | UnpackFailure | PersistenceFailure, Chunk[SubgraphMatchAtTime]] =
    for
      graph <- ZIO.service[Graph]
      _ <- graph.registerStandingQuery(subgraphSpec)

      graphLive = graph.asInstanceOf[GraphLive]
      testMatchSink = graphLive.matchSink.asInstanceOf[TestMatchSink]

      _ <- IngestableJson.ingestFromFile[T](filePath)(parallelism = 2)(using decoder)

      matches <- testMatchSink.matches
    yield matches

  val appConfigLayer: ULayer[AppConfig] = ZLayer.succeed {
    AppConfig(
      edgeReconciliation = EdgeReconciliationConfig(
        windowSize = 1.minute,
        windowExpiry = 2.minutes,
        maximumIntervalBetweenChunks = 1.minute
      ),
      matcher = MatcherConfig(resolveBindingsParallelism = 16),
      tracer = TracerConfig(enabled = true)
    )
  }

  def apply[T <: IngestableJson](
      jsonPath: String,
      subgraphSpec: SubgraphSpec,
      decoder: JsonDecoder[T]
  ): ZIO[Any, Throwable | UnpackFailure | PersistenceFailure, Unit] =
    (for
      graph <- ZIO.service[Graph]
      _ <- graph.start

      matches <- ingest(jsonPath, subgraphSpec, decoder)

      _ <- graph.stop

      _ <- ZIO.debug(s"matches: $matches")
    yield ()).provide(
      appConfigLayer,
      TemporaryRocksDB.layer,
      RocksDBNodeRepository.layer,
      NodeCache.layer,
      TestMatchSink.layer,
      EdgeSynchronizationFactory.layer,
      EdgeReconciliation.layer,
      RocksDBEdgeReconciliationRepository.layer,
      TracingService.live,
      Graph.layer,
      ContextStorage.fiberRef
    )
    