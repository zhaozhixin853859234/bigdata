package com.apiTest.dataStream.state

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.streaming.api.scala._

object OperatorWithStateTest {
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

//    val warningStream:DataStream[(String,Double,Double)] = inputStream
//      .keyBy("id")
//      .flatMapWithState()
  }

}