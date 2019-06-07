package com.qiao.sparkSql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    import com.qiao.util.MyPredef.deleteHdfs
    var outputPath = "data/people/output/"
    outputPath.deletePath()

    var inputPath = "data/people/input"
    val sqlc = new SQLContext(sc)

    val df = sqlc.read.json(inputPath)

    df.show()
    df.printSchema()

    df.select(df.col("age")).show()
    df.select(df.col("age").plus(1) , df.col("name").alias("名字")).show()

    df.filter(df.col("age").lt(25)).show()

    // select name,count(1) from table group by name
    val count = df.groupBy("name").count()
    count.show()
    count.printSchema()

    count.rdd.repartition(3).saveAsTextFile(outputPath)


  }


}
