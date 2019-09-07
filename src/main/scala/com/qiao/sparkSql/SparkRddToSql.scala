package com.qiao.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkRddToSql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRddToSql").setMaster("local[*]")
    val builder = SparkSession.builder().config(conf)
    val session = builder.getOrCreate()

    val data = session.sparkContext.makeRDD(Seq(1, 2, 3, 4, 5, 5))

    data.map(x => (x, 1)).reduceByKey(_ + _)

    data.collect()

  }

}
