package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 全量聚合
  */
object ProcessJob2 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result: DataStream[MinMaxTemp] = env.addSource(new SensorSource)
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(
        new MyAggFunc,
        new HighAndLowTempProcessFunction
      )


    result.print()
    env.execute()

  }


  class MyAggFunc extends AggregateFunction[(String, Double, Double), (String, Double, Double), (String, Double, Double)] {

    override def createAccumulator(): (String, Double, Double) = {
      ("", 500.0, 0.0)
    }

    override def add(in: (String, Double, Double), acc: (String, Double, Double)): (String, Double, Double) = {
      (in._1, in._2.min(acc._2), in._3.max(acc._3))
    }

    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc

    override def merge(acc1: (String, Double, Double), acc2: (String, Double, Double)): (String, Double, Double) = {
      (acc1._1, acc1._2.min(acc2._2), acc1._3.max(acc2._3))
    }
  }

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class HighAndLowTempProcessFunction extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         ctx: Context,
                         vals: Iterable[(String, Double, Double)],
                         out: Collector[MinMaxTemp]): Unit = {
      // val temps = vals.map(_._1)
      val windowEnd = ctx.window.getEnd

      out.collect(MinMaxTemp(key, vals.head._2, vals.head._3, windowEnd))
    }
  }

}
