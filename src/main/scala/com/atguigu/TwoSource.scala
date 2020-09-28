package com.atguigu

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object TwoSource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)
    val filterSwitches: DataStream[(String, Long)] = env.fromCollection(Seq(("sensor_2", 10 * 1000L), ("sensor_7", 20 * 1000L)))

    val forwardedReadings: DataStream[SensorReading] = readings.keyBy(_.id).connect(filterSwitches.keyBy(_._1))
      .process(new ReadingFilter)



    forwardedReadings.print()


    env.execute()

  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    // switch to enable forwarding
    // 传送数据的开关
    lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))

    // hold timestamp of currently active disable timer
    lazy val disableTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

    override def processElement1(reading: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // check if we may forward the reading
      // 决定我们是否要将数据继续传下去
      if (forwardingEnabled.value()) {
        out.collect(reading)
      }
    }

    override def processElement2(switch: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // enable reading forwarding
      // 允许继续传输数据
      forwardingEnabled.update(true)
      // set disable forward timer
      val timerTimestamp = ctx.timerService().currentProcessingTime() + switch._2
      val curTimerTimestamp = disableTimer.value()

      if (timerTimestamp > curTimerTimestamp) {
        // remove current timer and register new timer
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
        disableTimer.update(timerTimestamp)
      }
    }

    override def onTimer(ts: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      // remove all state; forward switch will be false by default
      forwardingEnabled.clear()
      disableTimer.clear()
    }
  }

}
