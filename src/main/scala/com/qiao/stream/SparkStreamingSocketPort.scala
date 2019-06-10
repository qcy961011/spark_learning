package com.qiao.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketPort ").setMaster("local[2]")

    val streamingContext = new StreamingContext(conf , Durations.seconds(5))

    val lines = streamingContext.socketTextStream("127.0.0.1" , 9999)

    val reduce: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    reduce.foreachRDD((r , t) => {
      println(s"count time : ${t} : ${r.collect().toList}")
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }


}
