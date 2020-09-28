package com.atguigu.app

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsJoin {

  // APP订单事件
  case class OrderEvent(orderId: String, eventType: String, eventTime: String)

  // 支付平台支付事件
  case class PayEvent(orderId: String, eventType: String, eventTime: String)

  // APP订单支付异常 侧输出流
  val unmatchedOrders = new OutputTag[OrderEvent]("unmatchedOrders")

  // 支付平台支付异常 侧输出流
  val unmatchedPays = new OutputTag[PayEvent]("unmatchedPays")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orders = env
      .fromCollection(List(
        //OrderEvent("1", "create", "1558430842"),
        //OrderEvent("2", "create", "1558430843"),
        OrderEvent("1", "pay", "1558430844") // 2019-05-21 17:27:24
        //OrderEvent("2", "pay", "1558430845"),
        //OrderEvent("3", "create", "1558430849"),
        //OrderEvent("3", "pay", "1558430849"),
        //OrderEvent("5", "pay", "1558430849") // 2019-05-21 17:27:29
      )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .filter(_.eventType == "pay")
      .keyBy(_.orderId)

    // 貌似与eventTime无关，之与当前的时间有关
    val pays = env.fromCollection(List(
      PayEvent("1", "weixin", "1558430843") // 2019-05-21 17:27:27
      //PayEvent("2", "zhifubao", "1558430848"),
      //PayEvent("4", "zhifubao", "1558430850"),
      //PayEvent("5", "bank", "9558430909") // 2019-05-21 17:28:29
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val processed = orders.connect(pays)
      .process(new EnrichmentFunction)

    processed.getSideOutput(unmatchedOrders).print()
    processed.getSideOutput(unmatchedPays).print()

    env.execute("TwoStreamsJoin")

  }

  class EnrichmentFunction extends CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)] {

    lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order", classOf[OrderEvent]))

    lazy val payState: ValueState[PayEvent] = getRuntimeContext.getState(new ValueStateDescriptor[PayEvent]("pay", classOf[PayEvent]))

    override def processElement1(order: OrderEvent,
                                 context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context,
                                 collector: Collector[(OrderEvent, PayEvent)]): Unit = {
      val pay: PayEvent = payState.value()
      if (pay != null) {
        payState.clear()
        collector.collect((order, pay))
      } else {
        orderState.update(order)
        context.timerService().registerEventTimeTimer(order.eventTime.toLong * 1000 + 5000L)
      }
    }

    override def processElement2(pay: PayEvent,
                                 context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context,
                                 collector: Collector[(OrderEvent, PayEvent)]): Unit = {
      val order: OrderEvent = orderState.value()
      if (order != null) {
        orderState.clear()
        collector.collect((order, pay))
      } else {
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime.toLong * 1000 + 5000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, PayEvent)]): Unit = {
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, orderState.value())
      } else if (payState.value() != null) {
        ctx.output(unmatchedPays, payState.value())
      }
      payState.clear()
      orderState.clear()
    }

  }

}
