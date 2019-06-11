package com.qiao.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamStatus {

  def main(args: Array[String]): Unit = {

    val checkPoint = "data/tmp/stream/SparkStreamStatus/output"
    val func: () => StreamingContext = () => {
      val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketportupdatestateobject").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext
    }


    val strc = StreamingContext.getOrCreate(checkPoint, func)
    strc.start()
    strc.awaitTermination()


  }
}
