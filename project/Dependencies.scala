import sbt.*
object Dependencies {

  object Versions {
    val zio = "2.0.13"
    val zioLogging = "2.1.13"
    val zioOpentelemetry = "3.0.0-RC10"
    val zioJson = "0.5.0"
    val zioConfig = "4.0.0-RC14"
    val zioConfigRefined = "4.0.0-RC14"
    val opentelemetry = "1.26.0"
    val opentelemetryNoop = "1.17.0-alpha"
    val jaeger = "1.8.1"
    val rocksDB = "8.1.1.1"
    val quill = "4.6.0.1"
    val msgPack = "0.9.3"
    val commonsIo = "20030203.000550"
  }

  object Orgs {
    val zio = "dev.zio"
    val opentelemetry = "io.opentelemetry"
    val jaegerTracing = "io.jaegertracing"
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
    Orgs.zio %% "zio-test" % Versions.zio % Test,
    Orgs.zio %% "zio-test-sbt" % Versions.zio % Test
  )

  lazy val opentelemetry: Seq[ModuleID] = Seq(
    Orgs.opentelemetry % "opentelemetry-api" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-context" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-exporter-jaeger" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-sdk" % Versions.opentelemetry,
    Orgs.opentelemetry % "opentelemetry-extension-noop-api" % Versions.opentelemetryNoop
  )

  lazy val jaegertracing: Seq[ModuleID] = Seq(
    Orgs.jaegerTracing % "jaeger-core" % Versions.jaeger,
    Orgs.jaegerTracing % "jaeger-client" % Versions.jaeger,
    Orgs.jaegerTracing % "jaeger-zipkin" % Versions.jaeger
  )

  lazy val persistence: Seq[ModuleID] = Seq(
    Orgs.quill %% "quill-jdbc-zio" % Versions.quill,
    Orgs.msgpack % "msgpack-core" % Versions.msgPack,
    Orgs.commonsIo % "commons-io" % Versions.commonsIo,
    Orgs.rocksDB % "rocksdbjni" % Versions.rocksDB
  )
}
