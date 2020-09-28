package com.atguigu.app

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random


case class MarketingUserBehavior(userId: String, behavior: String, channel: String, ts: Long)


class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true
  val channelSet = Seq("AppStore", "XiaomiStore", "HuaweiStore", "wechat")
  val behaviorTypes = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand = new Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString.replace("-", "")
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = Calendar.getInstance().getTimeInMillis
      sourceContext.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}


