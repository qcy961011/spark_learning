package com.qiao.stream

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingSocketPortHDFS {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingSocketPortHDFS").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf , Durations.seconds(5))
    val lines = streamingContext.socketTextStream("127.0.0.1" , 9999)
    val flatMap = lines.flatMap(_.split(" "))
    val mapToPair = flatMap.map((_,1))
    val reduceByKey = mapToPair.reduceByKey(_ + _)

    reduceByKey.foreachRDD(r => {
      if (!r.isEmpty()){
        val value = r.coalesce(1).mapPartitionsWithIndex((id, f) => {
          val hadoopConf = new Configuration()

          val fs = FileSystem.get(new URI("hdfs://ns1") , hadoopConf , "qcy96101111")
          val list: List[(String, Int)] = f.toList
          if (list.length > 0) {
            val format = new SimpleDateFormat("yyyyMMddHH").format(new Date())
            val path = new Path(s"hdfs://ns1/user/qiaoChunYu/sparkStreamTmpData/${id}_${format}")
            val outputStream: FSDataOutputStream = if (fs.exists(path)) {
              fs.append(path)
            } else {
              fs.create(path)
            }
            list.foreach(info => {
              outputStream.write(s"${info._1}\t${info._2}\n".getBytes("UTF-8"))
            })
            outputStream.close()
          }
          new ArrayBuffer().toIterator
        })
        // 以上的操作都是transformation操作，无法执行程序，
        // 所以添加一下action让程序执行
        value.foreachPartition(f => Unit)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
