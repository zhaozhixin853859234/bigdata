package com.apiTest.dataStream.sink

import java.util

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ESSinkTest {
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

    // 创建es连接主机和端口
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200));

    // 创建一个esSink的builder,注意addSink参数创建流程
    // 1、先利用ElasticsearchSink的builder创建一个esSinkBuilder对象
    // 2、创建该对象时传入EsSinkFunction类的实现对象，
    // 3、EsSinkFunction类中实现写入Es的逻辑

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("save data"+t)
          // 将数据包装成hashmap,准备数据
          val json = new util.HashMap[String,String]()
          json.put("sensor_id",t.id)
          json.put("temperature",t.temperature.toString)
          json.put("timestamp",t.timestamp.toString)

          // 创建index request，准备发送数据
          val indexrequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          // 利用requestIndexer参数发送请求，写入数据
          requestIndexer.add(indexrequest)

          println("data saved")
        }
      }
    )

    inputStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")
  }

}
