package com.oskarro

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducer {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  val dataOut: String = "Oskarek"


  def writeToKafka(topic:String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val record = new ProducerRecord[String,String](topic,"hello world")
    producer.send(record)
    producer.close()

  }

}
