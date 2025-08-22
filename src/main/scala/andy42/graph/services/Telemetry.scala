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

  /** Create an `OpenTelemetry` with a `TraceProvider` that exports via gRPC.
    *
    * The trace provider and span exporter resources are managed internally by the `OpenTelemetrySdk`.
    */
  private val grpcOpenTelemetry: RIO[Scope & AppConfig, api.OpenTelemetry] =
    for
      config <- ZIO.service[AppConfig]
      sdk <- ZIO.fromAutoCloseable(
        ZIO.succeed(
          OpenTelemetrySdk
            .builder()
            .setTracerProvider(
              SdkTracerProvider
                .builder()
                .setResource(
                  Resource.create(
                    Attributes.of(ServiceAttributes.SERVICE_NAME, config.tracing.instrumentationScopeName)
                  )
                )
                .addSpanProcessor(
                  SimpleSpanProcessor.create(
                    OtlpGrpcSpanExporter.builder().setEndpoint(config.tracing.host).build()
                  )
                )
                .build()
            )
            .build()
        )
      )
    yield sdk

  /** Create an `OpenTelemetry` with a `TraceProvider` that exports via JSON logging.
    *
    * The trace provider and span exporter resources are managed internally by the `OpenTelemetrySdk`.
    */
  private val jsonLoggingOpenTelemetry: RIO[Scope & AppConfig, api.OpenTelemetry] =
    for
      config <- ZIO.service[AppConfig]
      sdk <- ZIO.fromAutoCloseable(
        ZIO.succeed(
          OpenTelemetrySdk
            .builder()
            .setTracerProvider(
              SdkTracerProvider
                .builder()
                .setResource(
                  Resource.create(
                    Attributes.of(ServiceAttributes.SERVICE_NAME, config.tracing.instrumentationScopeName)
                  )
                )
                .addSpanProcessor(SimpleSpanProcessor.create(OtlpJsonLoggingSpanExporter.create()))
                .build()
            )
            .build()
        )
      )
    yield sdk

  /** Create an `OpenTelemetry` with a configured exporter.
    *
    * ZIO resource management is only applied to the `OpenTelemetry` object, and not all the object references it
    * contains (tracer providers, span exporters) since these are managed internally by the OpenTelemetry SDK.
    */
  private val openTelemetryLayer: RLayer[AppConfig, api.OpenTelemetry] =
    ZLayer.scoped {
      for
        config <- ZIO.service[AppConfig]
        otel <- config.tracing.exporter match
          case "noop"         => ZIO.succeed(api.OpenTelemetry.noop())
          case "grpc"         => grpcOpenTelemetry
          case "json-logging" => jsonLoggingOpenTelemetry
          case _ => ZIO.dieMessage(s"Invalid configuration for tracing.exporter: ${config.tracing.exporter}")
      yield otel
    }

  /** A service that produces a configured `Tracer`. This service is only used internally by ZIO Telemetry.
    */
  private val tracerLayer: RLayer[AppConfig & api.OpenTelemetry, Tracer] =
    ZLayer {
      for
        config <- ZIO.service[AppConfig]
        sdk <- ZIO.service[api.OpenTelemetry]
      yield sdk.getTracer(config.tracing.instrumentationScopeName)
    }

  /** A service that produces a configured `Tracing` object that is used to instrument code.
    */
  val configurableTracingLayer: RLayer[AppConfig, Tracing] =
    ZLayer.makeSome[AppConfig, Tracing](
      openTelemetryLayer,
      tracerLayer,
      OpenTelemetry.contextZIO,
      Tracing.live()
    )
