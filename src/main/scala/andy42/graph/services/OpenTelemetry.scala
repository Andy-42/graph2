package andy42.graph.services

import andy42.graph
import andy42.graph.config.AppConfig
import io.opentelemetry.api
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.semconv.ServiceAttributes
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

object OpenTelemetry:

  private def jaegerTracerProvider(host: String): RIO[Scope, SdkTracerProvider] =
    for
      spanExporter <- ZIO.fromAutoCloseable(ZIO.succeed(OtlpGrpcSpanExporter.builder().setEndpoint(host).build()))
      spanProcessor <- ZIO.fromAutoCloseable(ZIO.succeed(SimpleSpanProcessor.create(spanExporter)))
      tracerProvider <-
        ZIO.fromAutoCloseable(
          ZIO.succeed(
            SdkTracerProvider
              .builder()
              .setResource(Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, "opentelemetry-example")))
              .addSpanProcessor(spanProcessor)
              .build()
          )
        )
    yield tracerProvider

  private val openTelemetryLayer: RLayer[AppConfig, api.OpenTelemetry] =
    ZLayer.scoped {
      for
        config <- ZIO.service[AppConfig]
        tracerProvider <- jaegerTracerProvider(config.tracer.host)
        sdk <- ZIO.fromAutoCloseable(
          ZIO.succeed(
            OpenTelemetrySdk
              .builder()
              .setTracerProvider(tracerProvider)
              .build()
          )
        )
      yield if config.tracer.enabled then sdk else api.OpenTelemetry.noop()
    }

  private val tracerLayer: RLayer[AppConfig & api.OpenTelemetry, io.opentelemetry.api.trace.Tracer] =
    ZLayer {
      for
        sdk <- ZIO.service[api.OpenTelemetry]
        config <- ZIO.service[AppConfig]
        tracer = sdk.getTracer(config.tracer.instrumentationScopeName)
      yield tracer
    }

  val configurableTracerLayer: ZLayer[AppConfig, Throwable, Tracing] =
    ZLayer.makeSome[AppConfig, Tracing](
      openTelemetryLayer,
      tracerLayer,
      zio.telemetry.opentelemetry.OpenTelemetry.contextZIO,
      zio.telemetry.opentelemetry.tracing.Tracing.live()
    )
