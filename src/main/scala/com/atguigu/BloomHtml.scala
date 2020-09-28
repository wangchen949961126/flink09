package com.atguigu

import com.atguigu.HotGoods.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object BloomHtml {

  def main(args: Array[String]): Unit = {    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .readTextFile("input/UserBehavior.csv")
      .map(line => {
        val fields = line.split(",")
        UserBehavior(fields(0).toLong, fields(1).toLong, fields(2).toInt, fields(3), fields(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(r => ("dummyKey", r.userId.toString))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .trigger(new MyTrigger123)
      .process(new MyProcess)

    stream.print()
    env.execute()
  }

  class MyProcess extends ProcessWindowFunction[(String, String), (Long, String, String), String, TimeWindow] {
    lazy val jedis = new Jedis("localhost", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String,
                         context: Context,
                         vals: Iterable[(String, String)],
                         out: Collector[(Long, String, String)]): Unit = {
      val start: String = context.window.getStart.toString
      val storeKey = context.window.getEnd.toString
      var count = 0L

      if (jedis.hget("UvCountHashTable", storeKey) != null) {
        count = jedis.hget("UvCountHashTable", storeKey).toLong
      }

      val userId = vals.last._2
      val offset = bloom.hash(userId, 61)

      val isExist = jedis.getbit(storeKey, offset)
      if (!isExist) {
        jedis.setbit(storeKey, offset, true)
        jedis.hset("UvCountHashTable", storeKey, (count + 1).toString)
        out.collect((count + 1, start, storeKey))
      } else {
        out.collect((count, start, storeKey))
      }
    }
  }

  class MyTrigger123 extends Trigger[(String, String), TimeWindow] {
    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val key = window.getEnd.toString
        TriggerResult.FIRE_AND_PURGE
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

    override def onElement(element: (String, String),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
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

}
