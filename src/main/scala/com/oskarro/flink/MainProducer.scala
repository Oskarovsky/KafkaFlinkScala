package com.oskarro.flink

import com.oskarro.configuration.Constants
import com.oskarro.enums.VehicleType
import com.oskarro.enums.VehicleType.VehicleType
import com.oskarro.model.BusModel
import com.oskarro.services.KafkaService
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import net.liftweb.json.Serialization.write
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import play.api.libs.json.Json

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.concurrent.duration.DurationInt

object MainProducer {

  def main(args: Array[String]): Unit = {
    val system = akka.actor.ActorSystem("system")
    import system.dispatcher
    /*    system.scheduler.schedule(2 seconds, 10 seconds) {
      produceCurrentLocationOfVehicles("bus")
    }*/
    system.scheduler.schedule(1 seconds, 10 seconds) {
      produceCurrentLocationOfVehicles(VehicleType.tram)
    }
  }

  def produceCurrentLocationOfVehicles(vehicleType: VehicleType): Unit = {
    if (!VehicleType.values.toList.contains(vehicleType)) {
      throw new RuntimeException("There are API endpoints only for trams and buses")
    }

    val now = Calendar.getInstance().getTime
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss")
    println(s"[Timestamp - ${dataFormat.format(now)}] JSON Data for $vehicleType parsing started.")

    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> Constants.resourceID,
        "apikey" -> Constants.apiKey,
        "type" -> vehicleType.id.toString))

    val jsonObjectFromString = Json.parse(req.text)
    val response = jsonObjectFromString \ "result"

    implicit val formats: DefaultFormats.type = DefaultFormats
    val vehicleList = parse(response.get.toString()).extract[List[BusModel]]
    val kafkaService = new KafkaService()
    vehicleList foreach {
      veh =>
        kafkaService
          .writeToKafka(Constants.busTopic01, Constants.props, write(veh))
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
  }
}
