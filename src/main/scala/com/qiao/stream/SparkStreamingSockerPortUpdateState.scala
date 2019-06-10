package com.qiao.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object SparkStreamingSockerPortUpdateState {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketPort ").setMaster("local[2]")

    val streamingContext = new StreamingContext(conf , Durations.seconds(5))

    streamingContext.checkpoint("data/tmp/stream/updateState/output")

    val lines = streamingContext.socketTextStream("127.0.0.1" , 9999)

    val reduce: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val value = reduce.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
      var totla = 0

      for (i <- a) {
        totla += i
      }
      val last: Int = if (b.isDefined) b.get else 0
      val now: Int = totla + last
      Some(now)
    })


    value.foreachRDD((r , t) => {
      println(s"count time : ${t} : ${r.collect().toList}")
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
