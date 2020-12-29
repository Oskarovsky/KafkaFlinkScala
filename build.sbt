val flinkVer = "0.10.1"
val kafkaVer = "0.8.2.1"

libraryDependencies += "org.apache.flink" % "flink-core" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka-0.11_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-clients_2.11" % "1.11.3"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"

val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7"
)

val projectName = "KafkaFlinkScala"

lazy val main = Project(projectName, base = file("."))
  .settings(commonSettings)
  .settings(fork in run := true)
