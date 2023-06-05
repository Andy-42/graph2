import sbt.*
object Dependencies {

  object Versions {
    val openTelemetry = "1.26.0"
    val openTelemetryNoop = "1.17.0-alpha"
    val zio = "2.0.10"
    val zioLogging = "2.1.13"
    val zioOpentelemetry = "3.0.0-RC10"
    val zioJson = "0.3.0-RC10"
    val zioConfig = "4.0.0-RC14"
    val zioConfigRefined = "4.0.0-RC14"
    val rocksDB = "8.1.1.1"
    val quill = "4.6.0.1"
    val msgPack = "0.9.3"
    val cats = "2.7.0"
    val jaeger = "1.8.0"
    val zipkin = "2.16.3"
    val scalaCollectionCompat = "2.10.0"
    val commonsIo = "2.12.0"
  }

  object Orgs {
    val zio = "dev.zio"
    val openTelemetry = "io.opentelemetry"
    val jaegerTracing = "io.jaegertracing"
    val typelevel = "org.typelevel"
    val scalaLangModules = "org.scala-lang.modules"
    val quill = "io.getquill"
    val msgpack = "org.msgpack"
    val commonsIo = "commons-io"
    val rocksDB = "org.rocksdb"
  }

  lazy val zio = Seq(
    Orgs.zio %% "zio" % Versions.zio,
    Orgs.zio %% "zio-config" % Versions.zioConfig,
    Orgs.zio %% "zio-config-refined" % Versions.zioConfigRefined,
    Orgs.zio %% "zio-config-magnolia" % Versions.zioConfig,
    Orgs.zio %% "zio-config-typesafe" % Versions.zioConfig,
    Orgs.zio %% "zio-logging" % Versions.zioLogging,
    Orgs.zio %% "zio-opentelemetry" % Versions.zioOpentelemetry,
    Orgs.rocksDB % "rocksdbjni" % Versions.rocksDB,
    Orgs.commonsIo % "commons-io" % Versions.commonsIo,
    Orgs.zio %% "zio-test" % Versions.zio % Test,
    Orgs.zio %% "zio-test-sbt" % Versions.zio % Test
  )

  lazy val opentelemetry = Seq(
    Orgs.openTelemetry % "opentelemetry-api" % Versions.openTelemetry,
    Orgs.openTelemetry % "opentelemetry-context" % Versions.openTelemetry,
    Orgs.scalaLangModules %% "scala-collection-compat" % Versions.scalaCollectionCompat,
    Orgs.openTelemetry % "opentelemetry-exporter-jaeger" % Versions.openTelemetry,
    Orgs.openTelemetry % "opentelemetry-sdk" % Versions.openTelemetry,
    Orgs.openTelemetry % "opentelemetry-extension-noop-api" % Versions.openTelemetryNoop
  )

  lazy val jaegertracing = Seq(
    Orgs.jaegerTracing % "jaeger-core" % Versions.jaeger,
    Orgs.jaegerTracing % "jaeger-client" % Versions.jaeger,
    Orgs.jaegerTracing % "jaeger-zipkin" % Versions.jaeger
  )

  lazy val core = Seq(
    Orgs.quill %% "quill-jdbc-zio" % Versions.quill,
    Orgs.msgpack % "msgpack-core" % Versions.msgPack
  )
}
