package com.oskarro.training

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ColMap {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // first stream
    val first = env.fromElements((1, "1"))
    // second stream
    val second = env.fromElements((1, 1L))

    // connect streams
    val connected = first.keyBy(_._1).connect(second.keyBy(_._1))

    val printed = connected.map(new MyCoMap)

    printed.print()

    env.execute()
  }

  class  MyCoMap extends CoMapFunction[(Int,String),(Int,Long),String] {
    override def map1(in1: (Int, String)): String = {in1._2+"_1号流"}

    override def map2(in2: (Int, Long)): String = {in2._2.toString+"_2号流"}
  }
}
