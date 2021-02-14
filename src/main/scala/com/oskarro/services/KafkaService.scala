package com.oskarro.services

import com.oskarro.configuration.KafkaProperties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaService {

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

  def writeToKafka(info: String, topic: String, props: Properties = KafkaProperties.props, content: String): Unit = {

    // Send data on Kafka topic
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, content)
    producer.send(record)
    producer.close()
  }

}
