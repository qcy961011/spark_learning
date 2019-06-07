package com.qiao.sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkSqlDataSchema {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlDataSchema").setMaster("local[*]")
    val sc = new SparkContext(conf)

    import com.qiao.util.MyPredef.deleteHdfs
    var outputPath = "data/people/output/"
    outputPath.deletePath()

    var inputPath = "data/people/input"
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.json(inputPath)

  }
}
