package com.oskarro

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import play.api.libs.json.Json

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

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles("temat_oskar01", props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()
    val stream: DataStream[String] = env
      .addSource(consumer)

    stream.map(x => {
        val str = Json.parse(x)
        println(str)
      })

    env.execute("Flink Kafka Example")
  }


}
