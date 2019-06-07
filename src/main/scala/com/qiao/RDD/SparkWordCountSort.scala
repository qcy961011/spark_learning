package com.qiao.RDD

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCunt").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("debug")


    import com.qiao.util.MyPredef.deleteHdfs
    val output = "data/wordCount/output/"
    output.deletePath()

    val words = sc.textFile("data/wordCount/input" , 4).flatMap(_.split(" "))

    // spark 中的 sort 是个全局排序的sort ， 因为其自己实现了一个RangPartitioner ， 而 mr 想全局排序必须实现一个自己的partitioner
    val sort = words.filter(_.length > 0).map((_,1)).reduceByKey(_ + _ , 2).sortBy(_._2).map(x => s"${x._1}\t${x._2}")


    sort.saveAsTextFile(output)
    println(sort.toDebugString)

    sc.stop()
  }
}
