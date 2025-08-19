name := "graph2"

version := "0.1"

scalaVersion := "3.6.3"

libraryDependencies ++=
  Dependencies.zio ++
    Dependencies.opentelemetry ++
    Dependencies.jaegertracing ++
    Dependencies.persistence

scalacOptions ++= Seq("-deprecation")

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
