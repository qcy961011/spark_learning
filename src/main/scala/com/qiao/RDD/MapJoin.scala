package com.qiao.RDD

import org.apache.spark.{SparkConf, SparkContext}

object MapJoin {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
  }
}
