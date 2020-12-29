package com.oskarro

import scopt.OptionParser

object Main {

  case class Config(topic: String = "flink_input",
                    servers: String = "localhost:9092",
                    group: String = "test")


  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("scopt") {
      opt[String]('t', "topic").action((x, c) => c.copy(topic = x)).text("Topic to listen to")
      opt[String]('s', "servers").action((x, c) => c.copy(servers = x)).text("Kafka bootstrap servers")
      opt[String]('g', "group").action((x, c) => c.copy(servers = x)).text("Group id of the Kafka consumer")
    }

    KafkaProducer.writeToKafka("flink_input")

    parser.parse(args, Config()) match {
      case Some(config) =>
        new KafkaConsumer(config)

      case None =>
        println("Bad arguments")
    }
  }
}

