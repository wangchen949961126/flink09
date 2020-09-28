package com.atguigu

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaSource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result: DataStream[SensorReading] = env.addSource(new SensorSource)
    result.print()

    env.execute()

    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      "myTopic",
      new SimpleStringSchema(),
      properties))*/

  }

}
