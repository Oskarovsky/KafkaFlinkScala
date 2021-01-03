package com.oskarro

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import java.util.Properties

object Main {

  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    KafkaConsumer.readKafkaMessage("temat_oskar01", props)
//    KafkaProducer.writeToKafka("temat_oskar01", props, s"TEST CONTENT")

//    env.execute()
  }
}

