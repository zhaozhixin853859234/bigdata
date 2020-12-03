package com.wc
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取运行参数
    val params: ParameterTool = ParameterTool.fromArgs(args);
    val hostName: String =params.get("host")
    val port: Int = params.getInt("port")
    // 读取数据socket文件流, source
    val inputDataStream:DataStream[String] = env.socketTextStream(hostName,port)

    // transform
    val resultDataStream:DataStream[(String,Int)] = inputDataStream
//      .flatMap(_.split(" ")) //通配符
        .flatMap(str=>str.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    // sink
    resultDataStream.print()
    env.execute("wc Streaming")
  }

}
