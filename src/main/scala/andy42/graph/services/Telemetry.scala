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
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.tracing.Tracing

object Telemetry:

  private val jaegerTracerProvider: RIO[Scope & AppConfig, SdkTracerProvider] =
    for
      config <- ZIO.service[AppConfig]
      spanExporter <- ZIO.fromAutoCloseable(
        ZIO.succeed(OtlpGrpcSpanExporter.builder().setEndpoint(config.tracing.host).build())
      )
      spanProcessor <- ZIO.fromAutoCloseable(ZIO.succeed(SimpleSpanProcessor.create(spanExporter)))
      tracerProvider <-
        ZIO.fromAutoCloseable(
          ZIO.succeed(
            SdkTracerProvider
              .builder()
              .setResource(
                Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, config.tracing.instrumentationScopeName))
              )
              .addSpanProcessor(spanProcessor)
              .build()
          )
        )
    yield tracerProvider

  private val openTelemetryLayer: RLayer[AppConfig, api.OpenTelemetry] =
    ZLayer.scoped {
      for
        config <- ZIO.service[AppConfig]
        tracerProvider <- jaegerTracerProvider
        sdk <- ZIO.fromAutoCloseable(
          ZIO.succeed(
            OpenTelemetrySdk
              .builder()
              .setTracerProvider(tracerProvider)
              .build()
          )
        )
      yield if config.tracing.enabled then sdk else api.OpenTelemetry.noop()
    }

  private val tracerLayer: RLayer[AppConfig & api.OpenTelemetry, Tracer] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        sdk <- ZIO.service[api.OpenTelemetry]
        tracer = sdk.getTracer(config.tracing.instrumentationScopeName)
      yield tracer
    }

  val configurableTracingLayer: RLayer[AppConfig, Tracing] =
    ZLayer.makeSome[AppConfig, Tracing](
      openTelemetryLayer,
      tracerLayer,
      OpenTelemetry.contextZIO,
      Tracing.live()
    )
