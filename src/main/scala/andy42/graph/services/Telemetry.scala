package andy42.graph.services

import andy42.graph
import andy42.graph.config.AppConfig
import io.opentelemetry.api
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingSpanExporter
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

  private val grpcTracerProvider: RIO[Scope & AppConfig, SdkTracerProvider] =
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

  private val jsonLoggingTracerProvider: RIO[Scope & AppConfig, SdkTracerProvider] =
    for
      config <- ZIO.service[AppConfig]
      spanExporter   <- ZIO.fromAutoCloseable(ZIO.succeed(OtlpJsonLoggingSpanExporter.create()))
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


  private val grpcOpenTelemetry: RIO[Scope & AppConfig, api.OpenTelemetry] =
    for
      tracerProvider <- grpcTracerProvider
      sdk <- ZIO.fromAutoCloseable(
        ZIO.succeed(
          OpenTelemetrySdk
            .builder()
            .setTracerProvider(tracerProvider)
            .build()
        )
      )
    yield sdk

  private val jsonLoggingOpenTelemetry: RIO[Scope & AppConfig, api.OpenTelemetry] =
    for
      tracerProvider <- jsonLoggingTracerProvider
      sdk <- ZIO.fromAutoCloseable(
        ZIO.succeed(
          OpenTelemetrySdk
            .builder()
            .setTracerProvider(tracerProvider)
            .build()
        )
      )
    yield sdk

  private val openTelemetryLayer: RLayer[AppConfig, api.OpenTelemetry] =
    ZLayer.scoped {
      for
        config <- ZIO.service[AppConfig]
        otel <- config.tracing.exporter match
          case "noop" => ZIO.succeed(api.OpenTelemetry.noop())
          case "grpc" => grpcOpenTelemetry
          case "json-logging" => jsonLoggingOpenTelemetry
          case _      => ZIO.dieMessage(s"Invalid config.tracing.exporter: ${config.tracing.exporter}")
      yield otel
    }

  private val tracerLayer: RLayer[AppConfig & api.OpenTelemetry, Tracer] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        sdk <- ZIO.service[api.OpenTelemetry]
      yield sdk.getTracer(config.tracing.instrumentationScopeName)
    }

  val configurableTracingLayer: RLayer[AppConfig, Tracing] =
    ZLayer.makeSome[AppConfig, Tracing](
      openTelemetryLayer,
      tracerLayer,
      OpenTelemetry.contextZIO,
      Tracing.live()
    )
