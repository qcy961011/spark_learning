package com.qiao.sparkSql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveOrc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlHiveOrc").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)

    val inputPath = "data/orcData/input"

    val hivec = new HiveContext(sc)

    val df = hivec.read.orc(inputPath)

    df.printSchema()
    df.select(df.col("country")).show(5)
    df.select(df.col("country").as("load")).show(5)
    val count = df.groupBy("country").count()
    count.printSchema()
    val select = count.select(count.col("country"),count.col("count").as("num"))
    select.printSchema()
    val selectCount = select.filter(select.col("num").lt(10)).limit(5)
    selectCount.printSchema()
    val cache = selectCount.persist(StorageLevel.MEMORY_ONLY)
    cache.write.mode(SaveMode.Overwrite).format("orc").save("data/orcData/sqlOrcOut")
    cache.write.mode(SaveMode.Overwrite).format("json").save("data/orcData/sqJsonlOut")
    cache.write.mode(SaveMode.Overwrite).format("parquet").save("data/orcData/sqlParquetlOut")

  }
}
