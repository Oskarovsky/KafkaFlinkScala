val flinkVer = "0.10.1"
val kafkaVer = "0.8.2.1"

libraryDependencies += "org.apache.flink" % "flink-core" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-scala_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-connector-kafka-0.11_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-streaming-scala_2.11" % "1.11.3"
libraryDependencies += "org.apache.flink" % "flink-clients_2.11" % "1.11.3"

libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.8"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.4.6"
libraryDependencies += "net.liftweb" %% "lift-json" % "2.6-M4"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.32"

val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7"
)

val projectName = "KafkaFlinkScala"

lazy val main = Project(projectName, base = file("."))
  .settings(commonSettings)
  .settings(fork in run := true)
