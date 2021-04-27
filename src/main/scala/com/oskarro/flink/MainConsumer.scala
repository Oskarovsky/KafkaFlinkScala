package com.oskarro.flink

import com.oskarro.configuration.Constants
import com.oskarro.model.BusModel
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.json4s
import org.json4s.native.JsonMethods
import play.api.libs.json.{Json, Reads}

import java.sql.Timestamp
import java.util.Properties
import java.util.concurrent.TimeUnit

object MainConsumer {

  implicit val jsonMessageReads: Reads[BusModel] = Json.reads[BusModel]
  implicit lazy val formats: json4s.DefaultFormats.type = org.json4s.DefaultFormats

  private val AVERAGE_RADIUS_OF_EARTH_METER = 6371000

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(Constants.busTopic01, Constants.props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setParallelism(2)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()

    val busDataStream = env.addSource(kafkaConsumer)
      .filter { _.nonEmpty}
      .flatMap(line => JsonMethods.parse(line).toOption)
      .map(_.extract[BusModel])

    // a common operator to process different aggregation
    class CustomCountProc() extends ProcessWindowFunction[BusModel, BusModel, String, TimeWindow] {
      lazy val busState: ValueState[BusModel] = getRuntimeContext.getState(
        new ValueStateDescriptor[BusModel]("BusModel state", classOf[BusModel])
      )

      override def process(key: String, context: Context, elements: Iterable[BusModel], out: Collector[BusModel]): Unit = {
        for (e <- elements) {
          if (busState.value() != null) {
            out.collect(busState.value())
            val distance: Double = calculateDistance(e, busState.value())
            val duration: Double = calculateDuration(e, busState.value())
            println(
              s"===========\n" +
                s"Lon: ${e.Lon}, " +
                s"Lat: ${e.Lat}, " +
                s"Distance: $distance, " +
                s"Duration: $duration, " +
                s"Speed: ${calculateSpeed(distance, duration)}"
            )
          }
          busState.update(e)
          println(s"BusState: ${busState.value()}")
        }
      }
    }


    def calculateDistance(firstBus: BusModel, secondBus: BusModel): Double = {
      val latDistance = Math.toRadians(firstBus.Lat - secondBus.Lat)
      val lngDistance = Math.toRadians(firstBus.Lon - secondBus.Lon)
      val sinLat = Math.sin(latDistance / 2)
      val sinLng = Math.sin(lngDistance / 2)
      val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(firstBus.Lat)) *
          Math.cos(Math.toRadians(secondBus.Lat)) *
          sinLng * sinLng)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      (AVERAGE_RADIUS_OF_EARTH_METER * c)
    }

    def calculateDuration(firstBus: BusModel, secondBus: BusModel): Long = {
      val firstTime: Timestamp = Timestamp.valueOf(firstBus.Time)
      val secondTime: Timestamp = Timestamp.valueOf(secondBus.Time)
      val diffInMillis = firstTime.getTime - secondTime.getTime
      TimeUnit.MILLISECONDS.convert(diffInMillis, TimeUnit.MILLISECONDS)
    }

    def calculateSpeed(distance: Double, duration: Double): Double = {
      val distanceKm: Double = distance/1000
      val durationHour: Double = duration/3600000
      distanceKm/durationHour
    }

    val dataStream: DataStream[BusModel] = busDataStream
      .keyBy(_.VehicleNumber)
      .timeWindow(Time.seconds(10))
      .process(new CustomCountProc)


    createTypeInformation[(String, Double, Double, Timestamp, String, Double, Double, Double)]
    val sinkStream = dataStream
      .map(busRide => (
        java.util.UUID.randomUUID.toString,
        busRide.Lines.toDouble,
        busRide.Lon,
        Timestamp.valueOf(busRide.Time),
        busRide.VehicleNumber,
        busRide.Lat,
        busRide.Brigade.toDouble,
        1.44
      ))


    CassandraSink.addSink(sinkStream)
      .setQuery("INSERT INTO transport.bus_flink_speed(" +
        "\"Uuid\", " +
        "\"Lines\", " +
        "\"Lon\", " +
        "\"VehicleNumber\", " +
        "\"Time\", " +
        "\"Lat\", " +
        "\"Brigade\", " +
        "\"Speed\")" +
        " values (?, ?, ?, ?, ?, ?, ?, ?);")
      .setHost("localhost")
      .build()

//    dataStream.print.setParallelism(1)


    env.execute("Flink Kafka Example")
  }

}
