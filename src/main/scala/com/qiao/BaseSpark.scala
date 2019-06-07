package com.qiao

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 此配置文件只限于本地测试时使用
  * 线上运行时不可使用
  */
class BaseSpark {
  // 获取Spark的配置文件
  val conf = new SparkConf()
  // 设置应用程序名称，在程序运行的监控页面可以看淡
  conf.setAppName("selfTest")
  // 设置程序要连接的Spark集群的Master的URL
  // local为本地运行
  conf.setMaster("local[1]")
  // 获取Spark上下文
  val sc = new SparkContext(conf)
  sc.setLogLevel("Error")

}
