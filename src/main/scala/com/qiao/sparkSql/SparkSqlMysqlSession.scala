package com.qiao.sparkSql

import com.mysql.jdbc.Driver
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlMysqlSession {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val session:SparkSession = SparkSession.builder().config(conf).appName("SparkSqlMysqlSession").getOrCreate()

    val data = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn2.hadoop:3306/hainiucralwer")
      .option("dbtable", "hainiu_web_seed_externally")
      .option("user", "hainiu")
      .option("password", "12345678").load()

    data.createOrReplaceTempView("temp")
    val row: DataFrame = session.sql("select host from temp")
    row.show()
  }
}
