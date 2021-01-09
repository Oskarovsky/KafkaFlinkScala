package com.oskarro

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import net.liftweb.json.Serialization.write
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import play.api.libs.json.Json
import akka.actor.Actor
import akka.actor.Props

import java.text.SimpleDateFormat
import scala.concurrent.duration._
import java.util.{Calendar, Properties}
import java.util.concurrent.ScheduledThreadPoolExecutor

object MainProducer {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val apiKey: String = "b168711-aefd-4825-973a-4e1526c6ce93"
  val resourceID: String = "2e5503e-927d-4ad3-9500-4ab9e55deb59"

  def main(args: Array[String]): Unit = {
    val system = akka.actor.ActorSystem("system")
    import system.dispatcher
    system.scheduler.schedule(10 seconds, 10 seconds) {
      produceCurrentLocationOfVehicles("bus")
    }
  }

  def produceCurrentLocationOfVehicles(vehicleType: String): Unit = {
    case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

    var vehicleTypeNumber: String = ""
    if (vehicleType == "bus") {
      vehicleTypeNumber = "1"
    } else if (vehicleType == "tram") {
      vehicleTypeNumber = "2"
    } else {
      throw new RuntimeException("There are API endpoints only for trams and buses")
    }

    val now = Calendar.getInstance().getTime
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
    println(s"[Timestamp - ${dataFormat.format(now)}] -- JSON Data for $vehicleType parsing started.")
    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> "2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey" -> "3b168711-aefd-4825-973a-4e1526c6ce93",
        "type" -> vehicleTypeNumber))

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
