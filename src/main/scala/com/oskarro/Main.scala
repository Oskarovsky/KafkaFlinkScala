package com.oskarro

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import java.util.Properties

object Main {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    KafkaProducer.writeToKafka("temat_oskar01", props)
//    KafkaProducer.writeToKafka("temat_oskar02", props)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

/*    val stream = env.addSource(
      new FlinkKafkaConsumer010[String](
        "oskar_temat02",
        new SimpleStringSchema(),
        props
      )
    )
    stream.addSink(new FlinkKafkaProducer010[String](
      "localhost:9092",
      "oskar_temat02",
      new SimpleStringSchema()
    ))*/

/*    stream.print()
    env.execute()*/
  }
}

