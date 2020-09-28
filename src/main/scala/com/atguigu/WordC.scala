package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WordC {

  case class Wordwc(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val wordCount: DataStream[Wordwc] = text.flatMap(line => line.split(" "))
      .map(word => Wordwc(word, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    wordCount.print().setParallelism(1)
    env.execute()

  }

}
