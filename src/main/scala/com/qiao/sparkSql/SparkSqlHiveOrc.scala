package com.qiao.sparkSql

import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcStruct}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlHiveOrc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlHiveOrc").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions", "1")
    val session = SparkSession.builder.config(conf).getOrCreate()
    val inputPath = "data/orcData/input"

//    val df = session.read.orc(inputPath)
//    val rdd = session.sparkContext.hadoopFile(inputPath, Class[OrcNewInputFormat], Class[NullWritable], Class[OrcStruct])
//    rdd.take(3).foreach(x => {
//      x
//    })
//    df.createTempView("userpakage")
//    df.printSchema()
//    val data = session.sql(
//      """
//         | select
//         | a.id,
//         | sum(a.number)
//         |  from (
//         |  select
//         |    _col0 as id,
//         |    _col1 as page,
//         |    _col2 as timetemp,
//         |    _col3 as number,
//         |    _col4 as country,
//         |    _col5 as detaill,
//         |    _col6 as date
//         |  from
//         |    userpakage) a
//         | group by
//         | a.id
//      """.stripMargin)
//    data.show(5)
//    val count = df.groupBy("_col4").count()
//    count.printSchema()
//    val rows: Array[Row] = data.rdd.collect()
//    rows.foreach(row => {
//      print(row)
//    })

    //    count.show(5)
    //    val select = count.select(count.col("load"), count.col("count").as("num"))
    //    select.printSchema()
    //    val selectCount = select.filter(select.col("num").lt(10)).limit(5)
    //    selectCount.printSchema()
    //    val cache = selectCount.persist(StorageLevel.MEMORY_ONLY)
    //    cache.write.mode(SaveMode.Overwrite).format("orc").save("data/orcData/sqlOrcOut")
    //    cache.write.mode(SaveMode.Overwrite).format("json").save("data/orcData/sqJsonlOut")
    //    cache.write.mode(SaveMode.Overwrite).format("parquet").save("data/orcData/sqlParquetlOut")

  }
}


case class UserPakage(id:String , page:String , time:Long , number: Int , contry:String , n1:String , n2:String)
//case class UserPakage(_col0:String , _col6: Int , _col5:String , id:String , n1:String  , page:String , time:Long )
