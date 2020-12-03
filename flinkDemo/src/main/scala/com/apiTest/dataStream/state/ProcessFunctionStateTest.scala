package com.apiTest.dataStream.state

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{AggregatingState, ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val dataStream = env.socketTextStream("localhost",7777)
    val inputStream = dataStream
      .map(
        data=>{
          val array = data.split(",")
          SensorReading(array(0),array(1).toLong,array(2).toDouble)
        }
      )
      .keyBy("id")
      .process(new MyProcessFunctionState)

    inputStream.print("res")
    env.execute("process function state")

  }
}

class MyProcessFunctionState extends KeyedProcessFunction[Tuple,SensorReading,String]{

  // 两种创建状态的方法：
  // 1、懒加载
  //lazy val myState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("value state",classOf[Double]))
  // 2、使用富函数的初始化方法open(比较推荐)
  private var myValueState:ValueState[Double] = _
  private var myListState:ListState[Int] = _
  private var myMapState:MapState[String,Long] = _
  private var myReduceState:ReducingState[SensorReading] = _
  private var myAggregatingState:AggregatingState[SensorReading,SensorReading] = _

  override def open(parameters: Configuration): Unit = {
    myValueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("value state",classOf[Double]))
    myListState = getRuntimeContext.getListState(new ListStateDescriptor[Int]("list state",classOf[Int]))
    myMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("map state",classOf[String],classOf[Long]))
    myReduceState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
      "reducing state",
      new MyReducingFunction(),
      classOf[SensorReading]
    ))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 注意初始状态要确定好，要么进行并判断，否则输入第一条数据时，状态还没有进行更新计算，输出的是初始化的值

//    val valueTemp = myValueState.value()
//    myValueState.update(valueTemp+value.temperature)
//    // 这里只是将结果返回到datastream中，不会打印
//    out.collect("当前时间"+ctx.timerService().currentProcessingTime()+"传感器"+value.id+"温度累加之和为："+valueTemp)

    // 注意get方法只会获取最新的状态，不能获取之前的状态
    val reducingTemp = myReduceState.get()
    myReduceState.add(value)
    out.collect("前一次聚合结果："+reducingTemp)
    out.collect("当前聚合结果："+ myReduceState.get())

//    val mapTemp = myMapState.get(value.id.toString)
//    val keyState = myMapState.keys()

  }
}

class MyReducingFunction extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id,value2.timestamp.min(value1.timestamp),value2.temperature.max(value1.temperature))
  }
}
