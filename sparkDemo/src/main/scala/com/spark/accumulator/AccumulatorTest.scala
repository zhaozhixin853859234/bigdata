package com.spark.accumulator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello world","hello spark"))

    // 1、创建累加器
    val acc = new MyWordCountAccumulator

    // 2、注册累加器
    sc.register(acc)

    // 3、使用累加器
    rdd.flatMap(_.split(" ")).foreach(
      word=>{
        acc.add(word)
      }
    )

    // 4、获取累加器的值
    print(acc.value)

  }

  // abstract class AccumulatorV2[IN, OUT] extends Serializable
  // In:累加器输入值类型 Out：累加器输出类型
  class MyWordCountAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

    // 存储单词和次数
    var WordCountMap: mutable.Map[String, Int] = mutable.Map[String,Int]()

    // 累加器是否初始化
    override def isZero: Boolean = {
      WordCountMap.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyWordCountAccumulator
    }

    // 重置累加器
    override def reset(): Unit = {
      WordCountMap.clear()
    }

    // 自定义累加器的累加操作
    override def add(word: String): Unit = {
      WordCountMap(word) = WordCountMap.getOrElse(word,0)+1
    }

    // 合并累加器的值
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = WordCountMap
      val map2 = other.value

       WordCountMap = map1.foldLeft(map2)(
        (map,kv)=>{
          map(kv._1) = map.getOrElse(kv._1,0)+kv._2
          map
        }
      )
    }

    // 返回累加器的值
    override def value: mutable.Map[String, Int] = {
      WordCountMap
    }
  }
}
