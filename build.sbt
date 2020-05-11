
lazy val root = (project in file(".")).
  settings(
    name := "kafka-streams-dbextractor",
    version := "0.1",
    organization in ThisBuild := "ru.vtb",
    scalaVersion := "2.12.7",
    mainClass in Compile := Some("ru.vtb.kafka.streams.Main")
  )

val kafkfaVersion = "2.0.0"
scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.twitter" %% "finagle-http" % "20.4.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "io.circe" %% "circe-yaml" % "0.10.1",
  "io.circe" %% "circe-generic" % "0.10.1",
  "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "com.google.code.gson" % "gson" % "2.8.5",
  "org.apache.kafka" %% "kafka-streams-scala" % kafkfaVersion,
  "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts (Artifact("javax.ws.rs-api", "jar", "jar")), // this is a workaround for https://github.com/jax-rs/api/issues/571
  "org.scalatest" %% "scalatest" % "3.0.1" % Test)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}