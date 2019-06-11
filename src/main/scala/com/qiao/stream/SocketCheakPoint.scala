package com.qiao.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SocketCheakPoint {


  def main(args: Array[String]): Unit = {

    val checkPoint = "data/tmp/stream/updateStateCheakPoint/output"

    val strc = StreamingContext.getOrCreate(checkPoint, () => {

      val conf = new SparkConf().setAppName("SocketCheakPoint").setMaster("local[*]")

      val context = new StreamingContext(conf, Durations.seconds(5))

      context.checkpoint(checkPoint)


      val lines: ReceiverInputDStream[String] = context.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK)

      val words = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

      val updateState = words.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
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
      context
    })
    strc.start()
    strc.awaitTermination()


  }
}
