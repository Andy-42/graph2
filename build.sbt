name := "graph2"

version := "0.1"

scalaVersion := "3.2.2"

val zioV = "2.0.10"
val zioConfigRefinedV = "4.0.0-RC13"
val zioLoggingV = "2.1.10"
val quillV = "4.6.0.1"
val msgPackV = "0.9.3"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % zioV,
  "dev.zio" %% "zio-config-refined" % zioConfigRefinedV,
  "dev.zio" %% "zio-logging" % zioLoggingV,
  "io.getquill" %% "quill-jdbc-zio" % quillV,
  "org.msgpack" % "msgpack-core" % msgPackV,
  "dev.zio" %% "zio-test" % zioV % Test,
  "dev.zio" %% "zio-test-sbt" % zioV % Test,
  "dev.zio" %% "zio-test-magnolia" % zioV % Test
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")