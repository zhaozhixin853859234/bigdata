package com.apiTest.dataStream.sink

import java.util.Properties

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //val dataStream = env.readTextFile("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt")
    // 1.4、从消息中间件读取数据，实际应用中是这种实现（解耦、削峰），
    // 需要maven导入相关依赖
    // 定义配置项
    val properties = new Properties();
    // 连接的kafka集群
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092")
    // 开启自动提交offset，如果不自动提交偏移量，下一次会从kafka的_consumer_offset 默认存储offset主题
    // 读取offset，会发生重复读取数据
    //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
    // 自动提交延迟
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    // 反序列化类
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    // 消费者组,重置消费者组才能从头消费数据，--from-beginning效果
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "bd1")
    // 重置消费者offset
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


    val dataStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
    // 1、普通转换算子，单流操作
    // map先将一行数据分割转换到数组，然后封装SensorReading样例类
    val inputStream:DataStream[String] = dataStream.map(
      data=>{
        val dataArray = data.split(",")
        // 转成String方便序列化
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble).toString
      }
    )

    // 1、kafka Sink
    inputStream.addSink(new FlinkKafkaProducer011[String]("master:9092","sensorSink",new SimpleStringSchema()))
    inputStream.print("inputstream")
  }
}
