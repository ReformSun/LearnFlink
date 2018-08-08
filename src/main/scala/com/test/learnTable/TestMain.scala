package com.test.learnTable


import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TestMain {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataSource = sEnv.socketTextStream("localhost",9000);


    val input = dataSource.map(f=> {
      val arr = f.split("\\|")
      val code = arr(0)
      val time = arr(1).toLong
      (code,time)
    })


    val watermark = input.assignTimestampsAndWatermarks(assigner = new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentimemaxTimeTamp : Long = 0
      val maxOutOfOrderness = 1000L
      var a: Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentimemaxTimeTamp - maxOutOfOrderness)
        a
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {

        val timetamp = element._2
        currentimemaxTimeTamp = Math.max(timetamp,currentimemaxTimeTamp)
        timetamp
      }
    })

    val  window = watermark.keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new WindowFunctionTest)

    window.print()

    sEnv.execute()
  }


  class WindowFunctionTest extends WindowFunction[(String,Long),(String,Int,String,String,String,String),String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
      out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
    }
  }
}
