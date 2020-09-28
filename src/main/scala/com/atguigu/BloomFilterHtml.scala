package com.atguigu

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object BloomFilterHtml {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("input/UserBehavior.csv")
      .map(line => {
        val fields: Array[String] = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp * 1000)
      .map(r => ("dummyKey", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(10))
      .aggregate(new UvAggFunc, new UvProcessFunc)

    stream.print()
    env.execute()
  }

  //直接用聚合算子，【count，布隆过滤器】
  class UvAggFunc extends AggregateFunction[(String, Long), (Long, BloomFilter[lang.Long]), Long] {
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) = {
      (0, BloomFilter.create(Funnels.longFunnel(), 100000000, 0.01))
    }

    override def add(value: (String, Long),
                     accumulator: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
      var bloom: BloomFilter[lang.Long] = accumulator._2
      var uvCount = accumulator._1
      //通过布隆过滤器判断是否存在，不存在则+1
      if (!bloom.mightContain(value._2)) {
        bloom.put(value._2)
        uvCount += 1
      }
      (uvCount, bloom)
    }

    override def getResult(accumulator: (Long, BloomFilter[lang.Long])): Long = accumulator._1 //返回count

    override def merge(a: (Long, BloomFilter[lang.Long]), b: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???
  }

  class UvProcessFunc extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    // 连接到redis
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      // 窗口结束时间 ==> UV数
      // 窗口结束时间 ==> bit数组

      // 拿到key
      val start = new Timestamp(context.window.getStart)
      val end = new Timestamp(context.window.getEnd)
      out.collect(s"窗口开始时间为$start 到 $end 的uv 为 ${elements.head}")
    }


  }

}
