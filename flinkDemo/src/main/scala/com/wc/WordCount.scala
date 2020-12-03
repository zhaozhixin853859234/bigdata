package com.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 获取参数
    val params = ParameterTool.fromArgs(args)
    val inputPath = params.get("input")
    val outputPath = params.get("output")
    // data source
    val inputDataSet:DataSet[String] = env.readTextFile(inputPath)

    // data transform
    val resultDataSet :DataSet[(String,Int)]= inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    // data sink
    //resultDataSet.print()
    // 默认并行度为4（cpu核心数），所以会输出到四个不同的文件里，wcResult中四个文件
    // setParallelism(1)将sink操作并行度设置为1，只输出到一个文件中：1
    resultDataSet.writeAsText(outputPath).setParallelism(1)
    env.execute("WordCount")
  }

}
