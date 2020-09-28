package com.atguigu

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

object TestDemo01 {

  case class clickEvent(id: String, url: String, clickTime: String)

  class demoSource extends RichParallelSourceFunction[clickEvent] {

    var flag: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[clickEvent]): Unit = {
      while (flag) {
        sourceContext.collect(clickEvent("1", "http://www", "1583235132"))
      }
    }

    override def cancel(): Unit = {
      flag = false
    }
  }

}
