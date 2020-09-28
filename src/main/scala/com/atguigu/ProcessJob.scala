package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 全量聚合
  */
object ProcessJob {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result: DataStream[MinMaxTemp] = env.addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5)) // 每5秒求一次平均值
      .process(new HighAndLowTempProcessFunction)


    result.print()
    env.execute()

  }

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         ctx: Context,
                         vals: Iterable[SensorReading],
                         out: Collector[MinMaxTemp]): Unit = {
      val temps = vals.map(_.temperature)
      val windowEnd = ctx.window.getEnd

      out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
    }
  }

}
