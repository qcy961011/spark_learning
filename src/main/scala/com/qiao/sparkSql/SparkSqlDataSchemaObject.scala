package com.qiao.sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{RowFactory, SQLContext, SparkSession}


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
    val builder = SparkSession.builder.config(conf).enableHiveSupport()

    val session = builder.getOrCreate()
    val inputPath = "data/people/input"
    val df = session.read.json(inputPath)

//    val sqlc = new SQLContext(sc)

//    val df = session.createDataFrame(map,classOf[HainiuSqlData])

    df.printSchema()
    df.createOrReplaceTempView("qiao_table")

    val sql = session.sql("select * from qiao_table where name like \"%Andy%\"")
    sql.printSchema()
    sql.show()

  }
}
