package com.qiao.stream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamFile").setMaster("local[*]")
    val hadoopConf = new Configuration()

    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext= new StreamingContext(conf , Durations.seconds(5))
    val localPath = "data/sparkStream/testFile/input"

    val fileSystem = streamingContext.fileStream[LongWritable, Text, TextInputFormat](localPath, (path: Path) => {
      println(path)
      path.getName.endsWith("txt")
    }, false, hadoopConf)


    val flatMap = fileSystem.flatMap(_._2.toString.split(" "))
    val mapToPair = flatMap.map((_,1))

    val reduceBykey = mapToPair.reduceByKey(_ + _)

    reduceBykey.foreachRDD((r,t) => {
      println(s"count : ${t} , ${r.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
