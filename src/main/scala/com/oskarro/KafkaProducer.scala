package com.oskarro

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scalaj.http.{Http, HttpResponse}

import java.util.Properties

object KafkaProducer {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  val dataOut: String = "Oskarek"


  def writeToKafka(servers: String, topic:String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val response: HttpResponse[String] = Http("https://api.um.warszawa.pl/api/action/busestrams_get/")
      .param("resource_id", "f2e5503e-927d-4ad3-9500-4ab9e55deb59")
      .param("apikey", "3b168711-aefd-4825-973a-4e1526c6ce93")
      .param("type", "2")
      .asString

    val producer = new KafkaProducer[String,String](props)
    val record = new ProducerRecord[String,String](topic,"TEST HELLO WORLD !")
    val record2 = new ProducerRecord[String,String](topic, response.body)
    producer.send(record)
    producer.send(record2)
    producer.close()
  }

}
