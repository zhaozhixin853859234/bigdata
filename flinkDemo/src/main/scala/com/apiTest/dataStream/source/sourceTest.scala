package com.apiTest.dataStream.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.util.Random

object sourceTest {
  def main(args: Array[String]): Unit = {
    // 0、 创建执行环境（流处理环境、批处理环境）
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(3)

//    // 1.1、从集合中读取数据,数据在内存中固定，实际比较少用,做测试
//    val stream1: DataStream[SensorReading] =env.fromCollection(List(
//      SensorReading("sensor_1",1547718199,35.8),
//      SensorReading("sensor_6",1547718201,15.4),
//      SensorReading("sensor_7",1547718202,6.7),
//      SensorReading("sensor_10",1547718205,38.1),
//      SensorReading("sensor_11",1547718210,34.8),
//      SensorReading("sensor_1",1547718207,45),
//      SensorReading("sensor_1",1547718333,25.3),
//      SensorReading("sensor_1",1547718210,45.6)
//    ))
//    stream1.print("stream1")
//
//    // 1.2、从文件中读取数据
//    val stream2 = env.readTextFile("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt")
//    stream2.print("stream2")
//
//    // 1.3、socket文本流，并行度只能是1，很少用
//    val stream3 = env.socketTextStream("hostname",7777);
//    stream3.print("stream3")
//
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


//    val stream4 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
//    stream4.print("stream4")

    // 1.5、定义实现source,jdbc读取数据
    val stream5 = env.addSource(new MySensorSource())
    stream5.print("stream5")

//    val stream6 = env.addSource(new MyJDBCSource())
//    stream6.print("stream6")

    env.execute("source test")
  }
}

// 输入数据样例包装类
case class SensorReading(id:String,timestamp:Long,temperature:Double)

// 实现自定义sourceFunction
// 1、灵活操作各种来源数据
// 2、测试环境生成测试数据
class  MySensorSource extends SourceFunction[SensorReading]{
  // 定义flag表示数据源是否正常运行
  var running:Boolean = true

  override def cancel(): Unit = running=false

  // 随机生成sensor数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit ={
    // 定义一个随机数发生器
    val rand = new Random()

    // 随机生成10个传感器温度,正态分布生成初始温度
    var curTemp = 1.to(10).map(
      i=>("sensor_"+i,60 + rand.nextGaussian()*20)
    )

    // 无线循环生成随机数据流
    while(running){
      // map还是能成微小温度波动
      curTemp = curTemp.map (
        data=>(data._1,data._2+rand.nextGaussian())
      )

      val curTime= System.currentTimeMillis()

      // 包装样例类,用ctx发送数据sourceContext: SourceFunction.SourceContext[SensorReading]，run方法参数
      curTemp.foreach(
        data=>sourceContext.collect(SensorReading(data._1,curTime,data._2))
      )

      // 控制数据生成时间间隔
      Thread.sleep(1000L)
    }
  }
}

// jdbc source
class MyJDBCSource extends RichSourceFunction[SensorReading]{
  // 定义flag表示数据源是否正常运行
  var running:Boolean = true
  var conn:Connection = _
  var selectStmt:PreparedStatement = _
  var stmt:Statement = _

  // 初始化
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://master:3306/test","root","root")
    // 预编译执行环境
    selectStmt = conn.prepareStatement("select * from temperatures where temp>?")
    // sql字符串执行环境
    stmt = conn.createStatement()
  }

  // 执行业务逻辑，读取数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    selectStmt.setDouble(1,30)
    //val sql:String = "select * from temperatures"
    //val res:ResultSet = stmt.executeQuery(sql)
    val res:ResultSet = selectStmt.executeQuery()
    while (res.next() && running){
      // 读取数据
      val sensor = res.getString("sensor")
      val temp = res.getDouble("temp")
      val curTime= System.currentTimeMillis()
      // 把数据封装为样例类，并使用sourceContext发送数据
      sourceContext.collect(SensorReading(sensor,curTime,temp))

      // 控制数据生成时间间隔
      Thread.sleep(1000L)
    }
  }

  // 关闭资源
  override def cancel(): Unit = {
    running = false
    conn.close()
    selectStmt.close()
    stmt.close()
  }
}