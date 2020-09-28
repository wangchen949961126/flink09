package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 增量聚合（每5秒求一次平均温度）
  */
object IncJob {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result: DataStream[(String, Double)] = env.addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.minutes(1)) // 每5秒求一次平均值
      .aggregate(new MyAggFunc)
    result.print()

    env.execute()

  }

  class MyAggFunc extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {

    // 设定初始值
    override def createAccumulator(): (String, Double, Int) = {
      ("", 0.0, 0)
    }

    override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
      (in._1, in._2 + acc._2, acc._3 + 1)
    }

    override def getResult(acc: (String, Double, Int)): (String, Double) = {
      (acc._1, acc._2 / acc._3)
    }

    override def merge(acc1: (String, Double, Int), acc2: (String, Double, Int)): (String, Double, Int) = {
      (acc1._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
    }
  }

}
