package com.oskarro.flink

import com.oskarro.configuration.KafkaProperties
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser.parse
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.json4s.native.JsonMethods
import play.api.libs.json.{JsSuccess, Json, Reads}

import java.util.Properties

object MainConsumer {

  case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  implicit val jsonMessageReads: Reads[BusStream] = Json.reads[BusStream]
  implicit lazy val formats = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles("temat_oskar01", KafkaProperties.props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()

    val busDataStream = env.addSource(kafkaConsumer)
      .flatMap(raw => JsonMethods.parse(raw).toOption)
      .map(_.extract[BusStream])

    createTypeInformation[(String, Long, String, Long, String, Long, String)]

    // Creating bus data to sink into cassandraDB.
    val sinkBusDataStream = busDataStream
      .map(bus => (bus.Lines, bus.Lon, bus.VehicleNumber, bus.Time, bus.Lat, bus.Brigade))

    CassandraSink.addSink(sinkBusDataStream)
      .setHost("127.0.0.1")
      .setQuery("INSERT INTO metrics.buses_full(" +
        "\"Lines\", " +
        "\"Lon\", " +
        "\"VehicleNumber\", " +
        "\"Time\", " +
        "\"Lat\", " +
        "\"Brigade\")" +
        " values (?, ?, ?, ?, ?, ?);")
      .build()

//    busDataStream.map(x => {
//      val str = Json.parse(x)
//      println(str)
//      implicit val formats: DefaultFormats.type = DefaultFormats
//    })


    env.execute("Flink Kafka Example")
  }
}
