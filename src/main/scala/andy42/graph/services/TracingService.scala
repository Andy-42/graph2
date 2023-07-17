package andy42.graph.services

import andy42.graph
import andy42.graph.config.{AppConfig, TracerConfig}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.extension.noopapi.NoopOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import io.opentracing.noop.NoopTracer
import zio.*
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

object TracingService:

  private val tracerLive: RLayer[AppConfig, Tracer] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        tracerConfig = config.tracer

        tracer <- if tracerConfig.enabled then
          makeTracer(tracerConfig)
          else ZIO.succeed(NoopOpenTelemetry.getInstance().getTracer(tracerConfig.instrumentationScopeName))

      yield tracer
    }

  val live: RLayer[AppConfig, Tracing & Tracer] =
    tracerLive ++ ((tracerLive ++ ContextStorage.fiberRef) >>> Tracing.live)

  def makeTracer(tracerConfig: TracerConfig): Task[Tracer] =
    // TODO: Should these be managed?
    for
      spanExporter <- ZIO.attempt(OtlpGrpcSpanExporter.builder().setEndpoint(tracerConfig.host).build())
      spanProcessor <- ZIO.attempt(SimpleSpanProcessor.create(spanExporter))
      tracerProvider <-
        ZIO.attempt(
          SdkTracerProvider
            .builder()
            .setResource(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "streaming-graph")))
            .addSpanProcessor(spanProcessor)
            .build()
        )
      openTelemetry <- ZIO.attempt(OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build())
      tracer <- ZIO.attempt(openTelemetry.getTracer(tracerConfig.instrumentationScopeName))
    yield tracer
