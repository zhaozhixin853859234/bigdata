package com.apiTest.dataStream.window

import com.apiTest.dataStream.source.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object watermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.readTextFile("C:\\software\\code\\IdeaProject\\bigdata\\flinkDemo\\src\\main\\resources\\sensor.txt")
    val inputStream = dataStream.map(
      data=>{
        val array = data.split(",")
        SensorReading(array(0),array(1).toLong,array(2).toDouble)
      })
      // 1、周期性生成watermark方法，实现AssignerWithPeriodicWatermark接口，
      // 有fink自身实现的watermark生成方法，也可以自定义实现
      // 1.1、使用升序模式，将当前的timestamp作为最新的watermark，
      // 一般适用于顺序事件
      // .assignAscendingTimestamps(_.timestamp*1000L)

      // 1.2、使用flink提供的固定时间延迟的watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000L
      })
      .keyBy(0)
      .timeWindow(Time.seconds(1))

  }

}

// 1.3、自定义周期性生成watermark的assigner,传入自定义延迟时间Long类型,
// watermark = 最大时间戳 - 延迟时间
class MyPeriodicWatermark(lateness:Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  var maxTime = 0L
  var curTime = 0L
  override def getCurrentWatermark: Watermark = {
      new Watermark(maxTime-lateness)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    curTime=element.timestamp
    maxTime = math.max(curTime,maxTime)
    curTime
  }
}

// 2、非周期性生成watermark，用户根据自己需求，
// 使用特殊条件生成watermark，例如某个元素的状态，
// 需要实现AssignerWithPunctuatedWatermark接口，
class MyPunctuatedWatermark extends AssignerWithPunctuatedWatermarks[SensorReading]{
  // 先提取时间戳
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp

  // 再根据当前数据状态（某个条件）生成watermark
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.temperature>30)
      new Watermark(extractedTimestamp)
    else
      null
  }
}