package com.qiao.stream

import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ListBuffer

object SocketAndFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketPort ").setMaster("local[*]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val hadoopConf = new Configuration()
    val streamingContext = new StreamingContext(conf, Durations.seconds(10))

    streamingContext.checkpoint("data/tmp/stream/updateState/output")

    val lines = streamingContext.socketTextStream("127.0.0.1", 9999)

    val countryCountSocket: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val localPath = "data/sparkStream/contryDir/input"

    val fileSystem = streamingContext.fileStream[LongWritable, Text, TextInputFormat](localPath, (path: Path) => {
      path.getName.endsWith("dat")
    }, false, hadoopConf)

    //    val countryDictFile: DStream[(String, String)] = fileSystem.map((a: (LongWritable, Text)) => {
    //      val strings = a._2.toString.split("\t")
    //      (strings(0), strings(1))
    //    })


    val countryDictFile = fileSystem.mapPartitions(p => {
      var buffer = new ListBuffer[(String, String)]()
      val pattern = Pattern.compile("\t", 0)

      for (s <- p) {
        val strings = pattern.split(s._2.toString, 0)
        buffer += (strings(0) -> strings(1))
      }
      buffer.toIterator
    })
    //    countryCountSocket.join(countryDictFile).foreachRDD((r, t) => {
    //      r.foreach(f => {
    //        val countryCode: String = f._1
    //        val countryCount: Int = f._2._1
    //        val countryName: String = f._2._2
    //        println(s"time:${t},countryCode:${countryCode},countryCount:${countryCount},countryName:${countryName}")
    //      })
    //    })

    countryCountSocket.cogroup(countryDictFile).foreachRDD((r, t) => {
      r.foreach(f => {
        val countryCode: String = f._1
        val countryCount: Iterable[Int] = f._2._1
        val countryName: Iterable[String] = f._2._2
        println(s"time:${t},countryCode:${countryCode},countryCount:${countryCount},countryName:${countryName}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
