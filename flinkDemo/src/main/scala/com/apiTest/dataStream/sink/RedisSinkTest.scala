package com.apiTest.dataStream.sink

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.streaming.api.scala._

// 引入bahir提供的redis依赖，addSink方法中传入配置项和redisSink对象，实现sinkFunction
// redis是键值数据库
object RedisSinkTest {
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

    // redis sink
   // inputStream.addSink()

  }
}
