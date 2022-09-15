name := "graph2"

version := "0.1"

scalaVersion := "3.1.3"

val zioV = "2.0.2"
val zioConfigMagnoliaV = "3.0.2"
val zioLoggingV = "2.1.1"
val quillV = "4.4.1"
val msgPackV = "0.9.3"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % zioV,
  "dev.zio" %% "zio-config-magnolia" % zioConfigMagnoliaV,
  "dev.zio" %% "zio-logging" % zioLoggingV,
  "io.getquill" %% "quill-jdbc-zio" % quillV,
  "org.msgpack" % "msgpack-core" % msgPackV,
  "dev.zio" %% "zio-test" % zioV % Test,
  "dev.zio" %% "zio-test-sbt" % zioV % Test,
  "dev.zio" %% "zio-test-magnolia" % zioV % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")