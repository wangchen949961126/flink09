package com.atguigu


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotGoods {


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
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    stream.print()
    env.execute("Hot goods top 3")


  }


  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    lazy val itemState: ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
    )

    override def processElement(i: ItemViewCount,
                                context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                collector: Collector[String]): Unit = {
      itemState.add(i)
      // 注册定时器
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      // 导入一些隐式类型转换
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      itemState.clear()
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(-_.count).take(topSize)
      val result = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      result.append("窗口开始时间: ").append(new Timestamp(sortedItems.get(0).windowStart - 1)).append("\n")
      result.append("窗口结束时间: ").append(new Timestamp(sortedItems.get(0).windowEnd - 1)).append("\n")
      for (i <- sortedItems.indices) {
        val currentItem = sortedItems(i)
        result
          .append("NO")
          .append(i + 1)
          .append(":")
          .append("  商品ID=")
          .append(currentItem.itemId)
          .append("  浏览量=")
          .append(currentItem.count)
          .append("\n")
      }
      result.append("====================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }

  }


  class WindowResultFunction extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {

    override def process(key: Long,
                         context: Context,
                         elements: Iterable[Long],
                         out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getStart, context.window.getEnd, elements.iterator.next()))
    }

  }


  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2

  }


  // 全窗口聚合函数输出的数据类型
  case class ItemViewCount(itemId: Long, windowStart: Long, windowEnd: Long, count: Long)


  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

}
