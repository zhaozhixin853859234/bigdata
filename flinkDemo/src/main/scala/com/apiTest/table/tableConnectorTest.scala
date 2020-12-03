package com.apiTest.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings,TableEnvironment,DataTypes,Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object tableConnectorTest {
  def main(args: Array[String]): Unit = {
    // 1、创建执行环境，有新旧两套流程，新APi基于Blink实现批流统一
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建流式表执行环境,有几种不同配置
    val tableEnv = StreamTableEnvironment.create(env)

    // 1.1 旧流式处理执行环境,传入配置环 境参数
    val olderSetting = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val olderStreamTableEnv = StreamTableEnvironment.create(env,olderSetting)

    // 1.2 旧批式处理执行环境，重新创建批处理环境，再创建表执行环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 Blink新流式处理执行环境,传入配置环境参数,更体现出批流统一
    val blinkStreamSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSetting)

    // 1.4 Blink新批式处理执行环境,传入配置环境参数
    val blinkBatchSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSetting)

    // 2、连接外部系统读取数据
    // 2.1 读取文件数据
    val filePath = "C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt"
    // connect方法省略将文件读取为dataStream的过程
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
      .field("timestamp",DataTypes.BIGINT())
      .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") // 在表环境中注册一张表，此时不能使用Table API

    // 2.2 消费kafka数据
    tableEnv.connect(new Kafka()
    .version("0.11")
      .topic("sensor")
    .property("zookeeper.connect","master:2181")
    .property("bootstrap.servers","master:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 3、基于表做查询转换
    val resTable:Table= tableEnv.from("inputTable")
        .select('id,'temperature)
        //.filter('id==='sensor_1)


    // 4、输出
    // 4.1、输出到文件系统
    tableEnv.connect(new FileSystem().path("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor_output.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        //.field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable") // 在表环境中注册一张表，此时不能使用Table API

    resTable.insertInto("outputTable")
//    val inputTable:Table= tableEnv.from("inputTable")
//    val resTable1 = inputTable
//      .select("id,temperature")
//      .filter("id=='sensor_1'")
//    resTable1.toAppendStream[(String, Double)].print("output")
    env.execute(" connector test")
  }

}
