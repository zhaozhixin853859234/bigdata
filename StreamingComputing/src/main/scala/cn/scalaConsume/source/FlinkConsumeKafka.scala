package cn.scalaConsume.source

import java.util.Properties

import cn.scalaConsume.entry.ConsumeInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig


object FlinkConsumeKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g65")
    // 重置消费者offset
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


    val stream = env.addSource(new FlinkKafkaConsumer011[String]("consumeRecordV2",new SimpleStringSchema(),properties))
    val dataStream = stream.map(
      data=>{
        val dataArray = data.split(",")
        ConsumeInfo(dataArray(0),dataArray(1),dataArray(2),dataArray(3),dataArray(4),dataArray(5),dataArray(6).toInt)
      }
    )
      .keyBy("user_id")
      .keyBy("credit_type")
      .reduce((a,b)=>ConsumeInfo(a.user_id,b.credit_id,b.credit_type,b.consume_time,b.consume_city,b.consume_type,a.consume_money+b.consume_money))
      .map(data => data.user_id+","+data.credit_id+","+data.credit_type+","+data.consume_time+","+
        data.consume_city+","+data.consume_type+","+data.consume_money)
    dataStream.addSink(new FlinkKafkaProducer011[String]("master:9092","consumeSumRecord",new SimpleStringSchema()))

    dataStream.print("consumeRecord")
    env.execute("consumeRecord11")
  }
}

