package com.oskarro

import com.oskarro.Main.Config
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}

import java.util.Properties

class KafkaConsumer(config: Config) {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", config.servers)

  val stream: DataStream[String] = env
    .addSource(new FlinkKafkaConsumer011[String](config.topic, new SimpleStringSchema(), properties))

  stream
    .map((s: String) => s"This is a string: $s")
    .print

  env.execute("Flink Kafka Example")

}
