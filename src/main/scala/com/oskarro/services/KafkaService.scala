package com.oskarro.services

import com.oskarro.configuration.KafkaProperties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * KafkaService is a class that handle kafka process
 */

class KafkaService {


  /**
   * Creating environment for kafka that consumes stream message from kafka topic.
   * @param topic Kafka topic name for reading data.
   * @param properties Kafka basic configuration.
   */
  def readKafkaMessage(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val consumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    consumer.setStartFromLatest()
    val stream: DataStream[String] = env
      .addSource(consumer)
    env.execute("Flink Kafka Example")
  }


  /**
   * Creating environment for kafka that produces stream message for kafka topic.
   * @param topic Kafka topic name for writing data.
   * @param props Kafka basic configuration.
   * @param content Data processed during the cycle
   */
  def writeToKafka(topic: String, props: Properties = KafkaProperties.props, content: String): Unit = {
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, content)
    producer.send(record)
    producer.close()
  }

}
