package com.atguigu

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object LoginFailJob {

  case class LoginEvent(
                         userId: String,
                         ip: String,
                         eventType: String,
                         eventTime: String
                       )

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)


    // Pattern规则
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("aaa")
      .where(_.eventType.equals("fail"))
      .next("bbb") //紧挨着（followedBy不需要紧挨着）
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) // 事件发生在10s内


    // CEP.pattern(流，Pattern模板)
    val ps: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)


    // 产生输出流
    val result: DataStream[(String, String, String, String)] = ps
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("aaa", null).iterator.next()
        val second = pattern.getOrElse("bbb", null).iterator.next()
        (first.userId, first.ip, first.eventType, second.eventTime)
      })

    println("======================")
    result.print()
    println("======================")

    env.execute()

  }

}
