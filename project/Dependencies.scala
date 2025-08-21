import sbt.*
object Dependencies {

  object Versions {
    val zio = "2.1.20"
    val zioLogging = "2.5.1"
    val zioOpentelemetry = "3.1.8"
    val zioJson = "0.7.44"
    val zioConfig = "4.0.4"
    val zioConfigRefined = "4.0.4"
    val opentelemetry = "1.53.0"
    val openTelemetrySemconv = "1.34.0"
    val opentelemetryExportersLogging = "0.9.1"
    val jaeger = "1.8.1"
    val rocksDB = "10.2.1"
    val quill = "4.8.6"
    val msgPack = "0.9.10"
    val commonsIo = "2.20.0"
  }

  object Orgs {
    val zio = "dev.zio"
    val opentelemetry = "io.opentelemetry"
    val opentelemetrySemconv = "io.opentelemetry.semconv"
    val quill = "io.getquill"
    val msgpack = "org.msgpack"
    val commonsIo = "commons-io"
    val rocksDB = "org.rocksdb"
  }

  lazy val zio: Seq[ModuleID] = Seq(
    Orgs.zio %% "zio" % Versions.zio,
    Orgs.zio %% "zio-config" % Versions.zioConfig,
    Orgs.zio %% "zio-config-refined" % Versions.zioConfigRefined,
    Orgs.zio %% "zio-config-magnolia" % Versions.zioConfig,
    Orgs.zio %% "zio-config-typesafe" % Versions.zioConfig,
    Orgs.zio %% "zio-json" % Versions.zioJson,
    Orgs.zio %% "zio-logging" % Versions.zioLogging,
    Orgs.zio %% "zio-opentelemetry" % Versions.zioOpentelemetry,
    Orgs.zio %% "zio-opentelemetry-zio-logging" % Versions.zioOpentelemetry,
    Orgs.zio %% "zio-test" % Versions.zio % Test,
    Orgs.zio %% "zio-test-sbt" % Versions.zio % Test
  )

  lazy val opentelemetry: Seq[ModuleID] = Seq(
    Orgs.opentelemetry % "opentelemetry-api" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-context" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-sdk" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-sdk-trace" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-exporter-otlp" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-exporter-logging-otlp" % Versions.opentelemetry,
    Orgs.opentelemetrySemconv % "opentelemetry-semconv" % Versions.openTelemetrySemconv
  )

  lazy val persistence: Seq[ModuleID] = Seq(
    Orgs.quill %% "quill-jdbc-zio" % Versions.quill,
    Orgs.msgpack % "msgpack-core" % Versions.msgPack,
    Orgs.commonsIo % "commons-io" % Versions.commonsIo,
    Orgs.rocksDB % "rocksdbjni" % Versions.rocksDB
  )
}
