package com.apiTest.table

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object tableRegisterTest {
  def main(args: Array[String]): Unit = {
    // 0、创建流执行环境，并获取dataStream
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // data source
    val inputStream= env.readTextFile("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt")
    val dataStream:DataStream[SensorReading] = inputStream.map(
      data=>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      }
    )

    // 1、在流处理环境基础上创建表执行环境
    val tableEnv:StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2、基于表环境将dataStream转化为表
    val dataTable = tableEnv.fromDataStream(dataStream)

    // 3.1、调用table API，处理业务逻辑
    val resTable:Table = dataTable
      .select("id,temperature")
      .filter("id=='sensor_1'")

    // 3.2 使用sql处理业务逻辑
    tableEnv.registerTable("dataTable",dataTable)
   // tableEnv.connect().createTemporaryTable()
    val resSqlTable:Table = tableEnv.sqlQuery("select * from dataTable where id ='sensor_1'")


    // 4、表转换为dataStream打印，table不能打印print
    val resStream: DataStream[(String,Long, Double)] = resSqlTable.toAppendStream[(String,Long,Double)]
    resStream.print()

    env.execute("table test")
  }
}

