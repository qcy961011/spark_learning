package com.qiao.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkWindows {

  def main(args: Array[String]): Unit = {

    val checkPoint = "data/tmp/stream/SparkWindows/output"
    val func: () => StreamingContext = () => {
      val conf: SparkConf = new SparkConf().setAppName("SparkWindows").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext.checkpoint(checkPoint)
      val lines = streamingContext.socketTextStream("127.0.0.1" , 9999)
      val flatMap = lines.flatMap(_.split(" "))
      val mapToPair = flatMap.map((_,1))

      val value = mapToPair.window(Durations.seconds(20) , Durations.seconds(10)).countByValue()

      // 当不设置滑动窗口的时间时，默认滑动窗口为一个批次的时间
      val reduceBykey = mapToPair.reduceByKey(_ + _).window(Durations.seconds(20) , Durations.seconds(5))
      val updateState = reduceBykey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
        var totla = 0
        for (i <- a) {
          totla += i
        }
        val last: Int = if (b.isDefined) b.get else 0
        val now: Int = totla + last
        Some(now)
      })
      updateState.foreachRDD(r => {
        println(s"${r.collect().toList}")
      })
      streamingContext
    }


    val strc = StreamingContext.getOrCreate(checkPoint, func)
    strc.start()
    strc.awaitTermination()


  }
}
