package com.atguigu.app

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream: KeyedStream[OrderEvent, String] = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"), // 2019-05-21 17:27:22
      OrderEvent("2", "create", "1558430843"), // 2019-05-21 17:27:23
      OrderEvent("2", "pay", "1558430844"), // 2019-05-21 17:27:24
      OrderEvent("3", "pay", "1558430942"), // 2019-05-21 17:29:02
      OrderEvent("4", "pay", "1558430943") // 2019-05-21 17:29:03
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .followedBy("follow")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val orderTimeoutOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    val complateResult = patternStream.select(orderTimeoutOutput) {
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        var createOrder = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "timeout")
      }
    } {
      pattern: Map[String, Iterable[OrderEvent]] => {
        var payOrder = pattern.get("follow")
        OrderResult(payOrder.get.iterator.next().orderId, "success")
      }
    }

    complateResult.getSideOutput(orderTimeoutOutput).print()
    complateResult.print()

    env.execute()


  }

  case class OrderEvent(orderId: String, eventType: String, eventTime: String)

  case class OrderResult(orderId: String, eventType: String)


}
