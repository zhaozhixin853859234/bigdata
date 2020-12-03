package com.apiTest.dataStream.state

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._

object RichFunctionStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.socketTextStream("localhost",7777)
    val inputStream = dataStream
      .map(
        data=>{
          val array = data.split(",")
          SensorReading(array(0),array(1).toLong,array(2).toDouble)
        }
      )

      val warningStream:DataStream[(String,Double,Double)] = inputStream
      .keyBy("id")
      .map(new MyMapFunction(10))

    warningStream.print("res")
    env.execute("process function state")

  }
}

// 自定义mapFunction实现状态编程
class MyMapFunction(threshold:Double) extends RichMapFunction[SensorReading,(String,Double,Double)]{
  lazy val lastValue: ValueState[SensorReading] = getRuntimeContext.getState(new ValueStateDescriptor[SensorReading](" value state",classOf[SensorReading]))
  override def map(value: SensorReading): (String, Double, Double) = {
    val valueLast = lastValue.value()
    lastValue.update(value)
    val diff = (valueLast.temperature-value.temperature).abs
      if (diff>threshold){
        (value.id,valueLast.temperature,value.temperature)
      }
      else
        (value.id,0,0)
  }
}

