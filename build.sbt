name := "graph2"

version := "0.1"

scalaVersion := "3.1.3"

val zioV = "2.0.0"
val zioConfigV = "3.0.1"
val msgPackV = "0.9.1"
val quillV = "4.3.0" 

val scalaTestV = "3.2.12"
val scalaCheckV = "1.16.0"
val postgresqlV = "42.2.8"

libraryDependencies ++= Seq(
    "dev.zio" %% "zio" % zioV,
    "dev.zio" %% "zio-config-magnolia" % zioConfigV,
    "dev.zio" %% "zio-logging" % "2.1.0",

    "org.msgpack" % "msgpack-core" % msgPackV,

    "io.getquill" % "quill-jdbc-zio_3" % quillV,

    "org.scalatest" %% "scalatest" % scalaTestV % Test,
    "org.scalacheck" %% "scalacheck" % scalaCheckV % Test
)
