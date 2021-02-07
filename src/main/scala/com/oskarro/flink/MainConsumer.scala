package com.oskarro.flink

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

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserialization")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val apiKey: String = "3b168711-aefd-4825-973a-4e1526c6ce93"
  val resourceID: String = "2e5503e-927d-4ad3-9500-4ab9e55deb59"

  case class BusStream(Lines: String, Lon: Double, VehicleNumber: String, Time: String, Lat: Double, Brigade: String)

  implicit val jsonMessageReads: Reads[BusStream] = Json.reads[BusStream]
  implicit lazy val formats = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles("temat_oskar01", props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()
    val kafkaStream = env
      .addSource(consumer)

/*
    CassandraSink.addSink(sinkDataStream)
      .setHost("127.0.0.1")
      .setQuery("INSERT INTO metrics.test_stream(Testowy, Liczba) values (?, ?);")
      .build()
*/

    kafkaStream.map(x => {
      val str = Json.parse(x)
      println(str)
      implicit val formats: DefaultFormats.type = DefaultFormats
    })


    env.execute("Flink Kafka Example")
  }


}
