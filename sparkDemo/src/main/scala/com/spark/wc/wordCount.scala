package com.spark.wc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    // 1、创建上下文对象,首先创建配置对象,
    // 再将配置信息传入到SC对象构造方法中
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("wordCount")
    val sc = new SparkContext(conf)

    // 2、 实现业务操作
    // 2.1 读取数据,path参数可以读取文件夹下所有数据，默认采用HDFS路径，hadoopFile对象
    val fileRDD:RDD[String] = sc.textFile("C:\\software\\code\\IdeaProject\\bigdata\\sparkDemo\\src\\main\\data")
    // 2.2 转换数据,其中每一步转换算子的结果都是RDD类型
    val mapRDD: RDD[(String, Int)] = fileRDD
      .flatMap((line: String) => line.split(" "))
      .groupBy((word: String) => word)
      .map {
        case (word, iter) =>
          (word, iter.size)
      }

    // 2.3 不同实现方法,更通用
    // 理解def reduceByKey(func: (V, V) => V): RDD[(K, V)]
    // 按照key做分组，对V做聚合
    val reduceRDD: RDD[(String, Int)] = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // RDD转换为数组
    val wcArray: Array[(String, Int)] = reduceRDD.collect()

    // 2.4 输出数据
    println(wcArray.mkString(","))

    // 3、释放连接
    sc.stop()
  }

}
