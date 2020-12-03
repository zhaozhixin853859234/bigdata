package com.apiTest.dataStream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {
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

    inputStream.addSink(new MyJDBCSink())
    env.execute(" jdbc Sink")
  }
}

class MyJDBCSink() extends RichSinkFunction[SensorReading] {
  // 预先定义sql连接和预编译语句
  var conn: Connection = _
  var updateStmt:PreparedStatement = _
  var insertStmt:PreparedStatement = _

  // 初始化工作，创建jdbc连接，初始化预编译语句
  override def open(parameters: Configuration): Unit = {
    // 为什么要调用父类的open方法？
    super.open(parameters)
    println("start connection")
    conn = DriverManager.getConnection("jdbc:mysql://master:3306/test","root","root")
    insertStmt = conn.prepareStatement("insert into temperatures (sensor,temp) values(?,?)")
    updateStmt = conn.prepareStatement("update temperatures set temp = ? where sensor = ?")
  }

  // 执行预编译语句
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    println("start update data:"+value)
    updateStmt.execute()
    println("update compliment")
    if(updateStmt.getUpdateCount ==0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      println("start insert data:"+value)
      insertStmt.execute()
      println("insert compliment")
    }
  }

  // 关闭连接，清理工作
  override def close(): Unit = {
    print("all data saved! close connection")
    updateStmt.close()
    insertStmt.close()
    conn.close()
  }
}
