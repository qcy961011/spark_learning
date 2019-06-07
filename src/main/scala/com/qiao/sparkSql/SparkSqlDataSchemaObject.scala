package com.qiao.sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{RowFactory, SQLContext}


class HainiuSqlData(private var line:String) {
  def getLine()={
    line
  }
  def setLine(line:String)={
    this.line = line
  }
}



object SparkSqlDataSchemaObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    var inputPath = "data/people/input"

    val map = sc.textFile(inputPath).map(f => new HainiuSqlData(f))

    val sqlc = new SQLContext(sc)

    val df = sqlc.createDataFrame(map,classOf[HainiuSqlData])

    df.printSchema()
    df.createOrReplaceTempView("qiao_table")

    val sql = sqlc.sql("select * from qiao_table where line like \"%Andy%\"")
    sql.printSchema()
    df.show()

  }
}
