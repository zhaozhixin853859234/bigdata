package com.apiTest.dataStream.processFunction

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object processFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.socketTextStream("localhost",7777)
      .map(
      data=>{
        val array = data.split(",")
        SensorReading(array(0),array(1).toLong,array(2).toDouble)
      }
    )
    val warningStream:DataStream[String] = dataStream
      .keyBy("id")
      .process(new MyKeyedProcessFunction(10000L))

    warningStream.print()
    env.execute("process Function Test")
  }

}

// 实现统计10s内温度连续上升就报警（类比金融系统中指定时间段内消费次数过多，就报警）
class MyKeyedProcessFunction(interval:Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{

  // 定义状态存储上一条数据的温度信息和时间戳信息
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  lazy val lastTimeStampState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last TS",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先获取两个状态的值
    val lastTemp = lastTempState.value()
    val lastTS = lastTimeStampState.value()

    // 无论什么情况都要更新温度值为最新温度
    lastTempState.update(value.temperature)

    // 判断当前温度是否比上一次高，且定时器为空，则需要定义一个触发器，并把当前温度和时间写入状态
    if (value.temperature>lastTemp && lastTS==0){
      // 获取当前处理时间的时间戳，而不是数据中的事件时间
      val curProcessTime = ctx.timerService().currentProcessingTime()

      // 定义指定间隔后的定时器
      ctx.timerService().registerProcessingTimeTimer(curProcessTime+interval)
      lastTimeStampState.update(curProcessTime+interval)

    }

    // 如果当前温度小于上一次温度值，则删除定时器
    // 并把定时器时间戳状态清空（可以再一次进入lastTS==0条件）
    else if (value.temperature<lastTemp){
      ctx.timerService().deleteProcessingTimeTimer(lastTS)
      //清空状态
      lastTimeStampState.clear()
    }
    // 当温度大于上一次温度，且已经定义了定时器，则什么都不做

  }

  // 定时器触发后操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(timestamp+"时传感器 "+ctx.getCurrentKey+" 温度已经连续升高"+interval/1000+"秒")
    lastTimeStampState.clear()
  }
}
