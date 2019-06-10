package com.qiao.RDD

import com.qiao.BaseSpark
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer

object SparkBroadCaseDemo extends BaseSpark{
  def main(args: Array[String]): Unit = {

    val arr = ArrayBuffer(1, 2, 3, 4)

    val par = sc.parallelize(arr,4)

    val broad = sc.broadcast(arr)
    val acc = sc.accumulator(0)



    val reduceRes = par.map(f => {
      val value = broad.value
      println(s"broad${value.toList}")
      println(s"arr:${arr.toList}")
//      acc.add(1)  // 这里总值为4，但是看起来每次都是acc = 0,然后加1
      f
    }).reduce((a, b) => {
      arr += b + 1
      acc.add(1) //reduce第一次进入两个元素，所以会减少一次累加器
      a + b
    })


    println(reduceRes)
    println(arr) //本地会打印出来，但是集群环境不能使用

    println(s"acc:${acc}")

  }
}
