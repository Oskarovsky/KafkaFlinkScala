package com.oskarro

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducer {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")

  def writeToKafka(topic:String, props: Properties = properties, content: String): Unit = {

    // Send data on Kafka topic
    val producer = new KafkaProducer[String,String](props)
    val record = new ProducerRecord[String,String](topic, content)
    producer.send(record)
    producer.close()
  }

}
