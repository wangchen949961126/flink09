package com.atguigu

import java.lang

import com.atguigu.HotGoods.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloomFilter {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime事件时间
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
      .map(r => ("bloomKey", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .trigger(new MyTrigger)
      .process(new UvCountBloom)


    stream.print()
    env.execute("bloomFilter")

  }

  case class UvCount(windowEnd: Long, count: Long)

  class UvCountBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    lazy val jedis = new Jedis("localhost", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[UvCount]): Unit = {
      val storeKey = context.window.getEnd.toString
      var count = 0L
      if (jedis.hget("count", storeKey) != null) {
        count = jedis.hget("count", storeKey).toLong
      }
      val userId: String = elements.last._2.toString
      val offset: Long = bloom.hash(userId, 61)
      val isExist = jedis.getbit(storeKey, offset)
      if (!isExist) {
        jedis.setbit(storeKey, offset, true)
        jedis.hset("count", storeKey, (count + 1).toString)
      }
      out.collect(UvCount(storeKey.toLong, count))
    }
  }

  class Bloom(size: Long) extends Serializable {
    private val cap = size

    def hash(value: String, seed: Int): Long = {
      var result = 0
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
      (cap - 1) & result
    }
  }

  class MyTrigger extends Trigger[(String, Long), TimeWindow] {
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
  }

}
