package com.oskarro

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import net.liftweb.json.Serialization.write
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import play.api.libs.json.Json

import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.duration._

import java.util.Properties
import java.util.concurrent.ScheduledThreadPoolExecutor

object MainProducer {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  def main(args: Array[String]): Unit = {
    val system = akka.actor.ActorSystem("system")
    import system.dispatcher
    system.scheduler.schedule(10 seconds, 10 seconds) {
      getCurrentBuses()
    }
  }

  def getCurrentBuses(): Unit = {
    case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

    println("JSON Data parsing started...")
    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> "2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey" -> "3b168711-aefd-4825-973a-4e1526c6ce93",
        "type" -> "2"))

    val jsonObjectFromString = Json.parse(req.text)
    val response = jsonObjectFromString \ "result"

    implicit val formats: DefaultFormats.type = DefaultFormats
    val vehicleList = parse(response.get.toString()).extract[List[BusStream]]
    vehicleList foreach {veh => KafkaProducer.writeToKafka("VEHICLES_ACTION","temat_oskar01", Main.props, write(veh))}

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.execute()

  }
}
