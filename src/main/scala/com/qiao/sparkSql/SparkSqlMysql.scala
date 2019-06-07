package com.qiao.sparkSql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlMysql ").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlc = new SQLContext(sc)

    val jdbc = sqlc.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678","hainiu_web_seed_internally")
    val jdbc2 = sqlc.jdbc("jdbc:mysql://nn2.hadoop:3306/hainiucralwer?user=hainiu&password=12345678","hainiu_web_seed_externally")

    jdbc.createOrReplaceTempView("aa")
    jdbc2.createOrReplaceTempView("bb")

    val join = sqlc.sql("select * from aa a inner join bb b on a.md5 = b.md5 limit 100 ")
    join.show(100)
  }
}
