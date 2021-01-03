package com.oskarro

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scalaj.http.{Http, HttpResponse}
import spray.json._
import play.api.libs.json.Json

import java.util.Properties

object KafkaProducer {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  val dataOut: String = "Oskarek"


  def writeToKafka(topic:String, props: Properties = properties): Unit = {

/*    val response: HttpResponse[String] = Http("https://api.um.warszawa.pl/api/action/busestrams_get/")
      .param("resource_id", "f2e5503e-927d-4ad3-9500-4ab9e55deb59")
      .param("apikey", "3b168711-aefd-4825-973a-4e1526c6ce93")
      .param("type", "1")
      .asString*/

    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> "2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey" -> "3b168711-aefd-4825-973a-4e1526c6ce93",
        "type" -> "2"))

    val json = ujson.read(req.text)
    val json2 = req.text().parseJson


    val jsonObject = Json.parse(req.text)
    val result = jsonObject \ "result"
    println(result)


    val producer = new KafkaProducer[String,String](props)
//    val record = new ProducerRecord[String,String](topic,"TEST HELLO WORLD !")
//    val record2 = new ProducerRecord[String,String](topic, response.body)
//    val record3 = new ProducerRecord[String,String](topic, json.arr(0).)
//    producer.send(record)
//    producer.send(record2)
//    producer.send(record3)
    producer.close()
  }

}
