package com.oskarro

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonParser._
import net.liftweb.json.Serialization.write
import play.api.libs.json.Json
import spray.json._
object ReaderJson {

  def main(args: Array[String]): Unit ={

    case class BusStream(Lines: String,
                         Lon: Double,
                         VehicleNumber: String,
                         Time: String,
                         Lat: Double,
                         Brigade: String)


    // This is the code that is blowing up
    case class RootCollection(items: Array[BusStream]) extends IndexedSeq[BusStream]{
      def apply(index: Int): BusStream = items(index)
      def length: Int = items.length
    }

    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val ElementFormat = jsonFormat6(BusStream)
      implicit object RootCollectionFormat extends RootJsonFormat[RootCollection] {
        def read(value: JsValue): RootCollection = RootCollection(value.convertTo[Array[BusStream]])
        def write(f: RootCollection) = JsArray(f.toJson)
      }
    }

    import MyJsonProtocol._

    println("Running Parse JSON")
    val req = requests.get("https://api.um.warszawa.pl/api/action/busestrams_get/",
      params = Map(
        "resource_id" -> "2e5503e-927d-4ad3-9500-4ab9e55deb59",
        "apikey" -> "3b168711-aefd-4825-973a-4e1526c6ce93",
        "type" -> "2"))

    val jsonObject = Json.parse(req.text)
    val result = jsonObject \ "result"
    println("JSON string read:")
    println(result)

    val jsonObject2: JsValue = result.get.toString().parseJson
    println(jsonObject2)

    val jsonCollection = jsonObject2.convertTo[RootCollection]
    println(jsonCollection.apply(0))

    implicit val formats: DefaultFormats.type = DefaultFormats
    val credentials = parse(result.get.toString()).extract[List[BusStream]]
    credentials foreach {cred => KafkaProducer.writeToKafka("READER", "temat_oskar01", Main.props, write(cred))}

  }
}
