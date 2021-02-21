package com.oskarro.flink

import com.oskarro.configuration.Constants
import com.oskarro.model.BusModel
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

  implicit val jsonMessageReads: Reads[BusModel] = Json.reads[BusModel]
  implicit lazy val formats = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(Constants.busTopic01, Constants.props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()

    val busDataStream = env.addSource(kafkaConsumer)
      .flatMap(raw => JsonMethods.parse(raw).toOption)
      .map(_.extract[BusModel])

    createTypeInformation[(String, Long, String, Long, String, Long, String)]

    // Creating bus data to sink into cassandraDB.
    val sinkBusDataStream = busDataStream
      .map(bus => (bus.Lines, bus.Lon, bus.VehicleNumber, bus.Time, bus.Lat, bus.Brigade))

    CassandraSink.addSink(sinkBusDataStream)
      .setHost("localhost")
      .setQuery("INSERT INTO wawa.bus_stream_flink(" +
        "\"Line\", " +
        "\"Lon\", " +
        "\"VehicleNumber\", " +
        "\"Time\", " +
        "\"Lat\", " +
        "\"Brigade\")" +
        " values (?, ?, ?, ?, ?, ?);")
      .build()

/*    busDataStream.map(x => {
      val str = Json.parse(x)
      println(str)
      implicit val formats: DefaultFormats.type = DefaultFormats
    })*/


    env.execute("Flink Kafka Example")
  }
}
