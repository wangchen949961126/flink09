package com.atguigu

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ValueStateTemper {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val result: DataStream[(String, Double, Double)] = env.addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlertFunction(1.7))

    result.print()
    env.execute()

  }

  class TemperatureAlertFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
      lastTempState = getRuntimeContext.getState(lastTempDescriptor)
    }

    override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp: Double = lastTempState.value()
      val diff: Double = (in.temperature - lastTemp).abs
      if (diff >= threshold) {
        out.collect(in.id, in.temperature, diff)
      }
      lastTempState.update(in.temperature)
    }
  }

}
