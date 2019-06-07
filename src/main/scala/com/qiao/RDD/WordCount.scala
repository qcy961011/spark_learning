package com.qiao.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala extends App {


  // 获取Spark的配置文件
  val conf = new SparkConf()

  // 设置应用程序名称，在程序运行的监控页面可以看淡
  conf.setAppName("WordCount")

  // 设置程序要连接的Spark集群的Master的URL
  // local为本地运行
  conf.setMaster("local[*]")


  // 获取Spark上下文
  val sc = new SparkContext(conf)
  sc.setLogLevel("Error")

  // 取出文件中行数据
//  val lines = sc.textFile("data/wordCount/input", 1)
  val lines = sc.textFile("data/NumberWordCount/input")

  println(lines.getNumPartitions)
  //
  val words = lines.flatMap {
    line => line.split(" ")
  }

  val number = words.filter(x => x.length != 0)

//  val pairs = words.map { words => (words, 1) }

  val pairs = number.map(_ -> 1)

  private val wordCount: RDD[(String, Int)] = pairs.reduceByKey(_ + _)

  private val value = wordCount.sortBy(-_._2).map(x => s"${x._1}\t${x._2}").collect()

  value.foreach(println)

//  private val value: RDD[String] = wordCount.map(x => s"${x._1}\t${x._2} ")

//  wordCount.foreach(pair => println(pair._1 + " : " + pair._2))

//  value.foreach(println)

  sc.stop()

//  sc.textFile("hdfs://ns1/user/qiaoChunYu/data/wordCount" , 1).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).saveAsTextFile("hdfs://ns1/user/qiaoChunYu/output/spark/wordCount")
//  sc.textFile("hdfs://ns1/user/qiaoChunYu/data/wordCount" , 2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).foreach(pair => println(pair._1 + " : " + pair._2))
//  sc.textFile("hdfs://ns1/user/qiaoChunYu/data/wordCount" , 1).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).cache()

}
