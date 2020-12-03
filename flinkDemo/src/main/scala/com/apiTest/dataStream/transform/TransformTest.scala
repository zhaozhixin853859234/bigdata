package com.apiTest.dataStream.transform

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.readTextFile("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt")

    // 1、普通转换算子，单流操作
    // map先将一行数据分割转换到数组，然后封装SensorReading样例类
    val inputStream:DataStream[SensorReading] = dataStream.map(
      data=>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
      }
    )
      .filter(x=>x.temperature>30)

    // 2、滚动聚合算子，keyBy与聚合函数
    val keyByStream = inputStream
        .keyBy(_.id)
//        .minBy(2)
      // 新来的数据与之前的统计数据进行合并
        .reduce((x,y)=>SensorReading(x.id,x.timestamp+10,y.temperature.min(x.temperature)))
    keyByStream.print("keyBystream")


    // 3、多流操作，分流与和流



    env.execute("transform")

  }

}
