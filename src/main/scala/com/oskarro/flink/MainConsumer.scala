package com.oskarro.flink

import com.oskarro.configuration.Constants
import com.oskarro.model.BusModel
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.cassandra.CassandraSink.CassandraSinkBuilder
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.util.Collector
import org.json4s
import org.json4s.native.JsonMethods
import play.api.libs.json.{Json, Reads}

import java.lang
import java.util.Properties

object MainConsumer {

  implicit val jsonMessageReads: Reads[BusModel] = Json.reads[BusModel]
  implicit lazy val formats: json4s.DefaultFormats.type = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    readCurrentLocationOfVehicles(Constants.busTopic01, Constants.props)
  }

  def readCurrentLocationOfVehicles(topic: String, properties: Properties): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setParallelism(2)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()

    val busDataStream: DataStream[BusModel] = env.addSource(kafkaConsumer)
      .flatMap(raw => JsonMethods.parse(raw).toOption)
      .map(_.extract[BusModel])

    busDataStream.print()

    createTypeInformation[(String, Long, String, Long, String, Long, String)]

    /* JSON END REGION */



/*    // a common operator to process different aggregation
    class CustomCountProc() extends ProcessWindowFunction[BusModel, BusModel, String, TimeWindow] {

      override def process(key: String, context: Context, elements: Iterable[BusModel], out: Collector[BusModel]): Unit = {
        print("AAAAA")
        for (e <- elements) {
          print("AAAAA")
          out.collect(BusModel(e.Lines, e.Lon, e.VehicleNumber, e.Time, e.Lat, e.Brigade))
        }

      }
    }

    busDataStream
      .keyBy(_.Brigade)
      .timeWindow(Time.seconds(10))
      .process(new CustomCountProc)
      .print()*/

    env.execute("Flink Kafka Example")


    /* CASSANDRA */
    // Creating bus data to sink into cassandraDB.
    val sinkBusDataStream = busDataStream
      .map(
        bus => (
          java.util.UUID.randomUUID.toString,
          bus.Lines,
          bus.Lon,
          bus.VehicleNumber,
          bus.Time,
          bus.Lat,
          bus.Brigade
        )
      )

    val sinkBuilder: CassandraSinkBuilder[(String, String, Double, String, String, Double, String)] =
      CassandraSink.addSink(sinkBusDataStream)

    sinkBuilder
      .setHost("localhost")
      .setQuery("INSERT INTO transport.bus_flink(" +
        "\"Uuid\", " +
        "\"Lines\", " +
        "\"Lon\", " +
        "\"VehicleNumber\", " +
        "\"Time\", " +
        "\"Lat\", " +
        "\"Brigade\")" +
        " values (?, ?, ?, ?, ?, ?, ?);")
      .build()

    env.execute("Flink Kafka Example")
  }

}
