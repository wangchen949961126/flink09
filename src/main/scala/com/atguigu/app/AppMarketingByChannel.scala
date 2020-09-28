package com.atguigu.app

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)



    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(data => ((data.channel, data.behavior), 1L))
      //.map(data => ((data.behavior, ""), 1L))
      .keyBy(_._1)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .process(new MarketingCountByChannel)

    stream.print()
    env.execute()

  }


  case class MarketingCountView(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)


  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingCountView, (String, String), TimeWindow] {

    private def format(ts: Long): String = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.format(ts)
    }

    override def process(key: (String, String),
                         context: Context,
                         elements: Iterable[((String, String), Long)],
                         out: Collector[MarketingCountView]): Unit = {
      out.collect(MarketingCountView(
        format(context.window.getStart),
        format(context.window.getEnd),
        key._1,
        key._2,
        elements.size))
    }

  }

}
