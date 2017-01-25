name := "StreamingJsonEnrichment"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.2"

libraryDependencies ++=
    Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.7",
      "com.github.benfradet" %% "spark-kafka-0-10-writer" % "0.2.0",
      "com.rollbar" % "rollbar" % "0.5.3",
      "com.typesafe" % "config" % "1.2.1",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "org.apache.kafka" %% "kafka" % "0.10.1.1",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.2",
      "org.rogach" %% "scallop" % "2.0.2"
    )

mainClass in assembly := Some("onextent.streaming.json.Enrichment")

assemblyMergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("com",   "google", xs @ _*) => MergeStrategy.last
  case PathList("com",   "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("io",    "netty", xs @ _*) => MergeStrategy.last
  case PathList("org",   "slf4j", xs @ _*) => MergeStrategy.last
  case PathList("org",   "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// scala lint
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value

(compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle

lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value

