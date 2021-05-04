package com.oskarro.flink

import com.oskarro.configuration.Constants
import com.oskarro.model.{BusModel, BusRideModel}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.json4s
import org.json4s.native.JsonMethods
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{Json, Reads}

import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit

object MainConsumer {

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
  implicit val jsonMessageReads: Reads[BusRideModel] = Json.reads[BusRideModel]
  implicit lazy val formats: json4s.DefaultFormats.type = org.json4s.DefaultFormats

  private val AVERAGE_RADIUS_OF_EARTH_METER = 6371000

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(Constants.busTopic01, Constants.props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setParallelism(3)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()

    val busDataStream = env.addSource(kafkaConsumer)
      .filter { _.nonEmpty}
      .flatMap(line => JsonMethods.parse(line).toOption)
      .map(_.extract[BusRideModel])

    // a common operator to process different aggregation
    class CustomCountProc() extends ProcessWindowFunction[BusRideModel, BusRideModel, String, TimeWindow] {
      lazy val busState: ValueState[BusRideModel] = getRuntimeContext.getState(
        new ValueStateDescriptor[BusRideModel]("BusModel state", classOf[BusRideModel])
      )

      override def process(key: String, context: Context, elements: Iterable[BusRideModel], out: Collector[BusRideModel]): Unit = {
        for (e <- elements) {
          var vehicleSpeed: Double = 1.234
          if (busState.value() != null) {
            out.collect(busState.value())
            val distance: Double = calculateDistance(e, busState.value())
            val duration: Double = calculateDuration(e, busState.value())
            vehicleSpeed = calculateSpeed(distance, duration)
            logger.info(
              s"===========\n" +
                s"Lon: ${e.Lon}, " +
                s"Lat: ${e.Lat}, " +
                s"Distance: $distance, " +
                s"Duration: $duration, " +
                s"Speed: ${calculateSpeed(distance, duration)}"
            )
          }
          val tempState: BusRideModel = BusRideModel(
            e.Lines, e.Lon, e.VehicleNumber, e.Time, e.Lat, e.Brigade, vehicleSpeed
          )
          busState.update(tempState)
          logger.info(s"BusState: ${busState.value()}")
        }
      }
    }


    def calculateDistance(firstBus: BusRideModel, secondBus: BusRideModel): Double = {
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

    def calculateDuration(firstBus: BusRideModel, secondBus: BusRideModel): Long = {
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

    val dataStream: DataStream[BusRideModel] = busDataStream
      .keyBy(_.VehicleNumber)
      .timeWindow(Time.seconds(10))
      .process(new CustomCountProc)


    createTypeInformation[(String, Double, Double, java.util.Date, String, Double, Double, Double)]

    val sinkStream = dataStream
      .map(busRide => (
        java.util.UUID.randomUUID.toString,
        busRide.Lines.toDouble,
        busRide.Lon,
        busRide.VehicleNumber,
        Timestamp.from(Instant.now()),
        busRide.Lat,
        busRide.Brigade.toDouble,
        busRide.Speed
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

    env.execute("Flink Kafka Example")
  }

}
