package com.qiao.RDD

import org.apache.spark.{SparkConf, SparkContext}

object LogSort {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("LogSort"))

    val data = sc.textFile("data/log/input")

    val value = data.map(x => {
      val strings = x.split(" ")
      ((strings(1), strings(4)), 1)
    })
      .reduceByKey(_ + _)
      .map(x => {
        (x._1._1, (x._1._2, x._2))
      })
      .groupByKey()
      .map(x => {
        (x._1, x._2.toList.sortWith((a, b) => {
          a._2 > b._2
        }).take(3))
      })

    value.foreach(x => println(x))
  }
}
