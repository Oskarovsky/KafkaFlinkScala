package com.oskarro

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

object KafkaConsumer {

  def readKafkaMessage(topic: String, properties: Properties): Unit = {

    // Fetching data from kafka
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()
    val stream: DataStream[String] = env
      .addSource(consumer)



    stream.print

    env.execute("Flink Kafka Example")
  }
}
