package com.atguigu

import java.util.Calendar

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random
import org.apache.flink.util.Collector

object RandT {

  // 传感器id，时间戳，温度
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  // 需要extends RichParallelSourceFunction, 泛型为SensorReading
  class SensorSource extends RichParallelSourceFunction[SensorReading] {

    // flag indicating whether source is still running.
    // flag: 表示数据源是否还在正常运行
    var running: Boolean = true

    /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
    // run()函数连续的发送SensorReading数据，使用SourceContext
    // 需要override
    override def run(srcCtx: SourceContext[SensorReading]): Unit = {

      // initialize random number generator
      // 初始化随机数发生器
      val rand = new Random()
      // look up index of this parallel task
      // 查找当前运行时上下文的任务的索引
      val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

      // initialize sensor ids and temperatures
      // 初始化10个(温度传感器的id, 温度值)元组
      var curFTemp = (1 to 10).map {
        // nextGaussian产生高斯随机数
        i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
      }

      // emit data until being canceled
      // 无限循环，产生数据流
      while (running) {
        // update temperature
        // 更新温度
        curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
        // get current time
        // 获取当前时间戳
        val curTime = Calendar.getInstance.getTimeInMillis

        // emit new SensorReading
        // 发射新的传感器数据, 注意这里srcCtx.collect
        curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

        // wait for 100 ms
        Thread.sleep(100)
      }

    }

    /** Cancels this SourceFunction. */
    // override cancel函数
    override def cancel(): Unit = {
      running = false
    }

  }

  class WarningFunc extends KeyedProcessFunction[String, SensorReading, String] {
    // 保存上一个传感器温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
    )
    // 保存注册的定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      // get previous temperature
      // 取出上一次的温度
      val prevTemp = lastTemp.value()
      // update last temperature
      // 将当前温度更新到上一次的温度这个变量中
      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()
      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        // temperature decreased; delete current timer
        // 温度下降或者是第一个温度值，删除定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        // 清空状态变量
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
        // temperature increased and we have not set a timer yet
        // set processing time timer for now + 1 second
        // 温度上升且我们并没有设置定时器
        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        // remember current timer
        currentTimer.update(timerTs)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    /* *********************************************************************************** */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    /* *********************************************************************************** */

    val warnings: DataStream[String] = stream.keyBy(_.id)
      .process(new WarningFunc)

    warnings.print()

    env.execute()

  }

}
