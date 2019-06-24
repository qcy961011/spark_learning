package com.qiao.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkTablePutPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparktableput").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val orcPath = "data/orcData/input/part-r-00000"

    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)
    df.createOrReplaceTempView("user_install_status")
    val sql: DataFrame = hivec.sql("select pkgname,count(1) from user_install_status group by pkgname")
    val rdd: RDD[Row] = sql.rdd
    //这里打印的是200，因为spark-sql的shuffle的时候默认的partition是200，所以这个DF转出来的RDD就是200个partition
    println(rdd.getNumPartitions)

    val repartition: RDD[Row] = rdd.repartition(300)

    val acc = sc.longAccumulator

    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = repartition.mapPartitions(f => {
      val list: List[Row] = f.toList
      //      println(list)
      //因为上面给它变成了300个partition，所以这里的累加器就会累加300次
      acc.add(1L)
      import scala.collection.mutable.ListBuffer
      val list1 = new ListBuffer[(ImmutableBytesWritable, Put)]

      //如果把在外面new ImmutableBytesWritable()的话就会造成，元组拿到的都是循环中最后的数据的指向
      //就会造成list1里面的数据都是最后一次keyOut.set(put.getRow)的结果了
      for (next <- list) {
        val keyOut = new ImmutableBytesWritable()
        val put = new Put(Bytes.toBytes("spark_tab_0620" + next.getString(0)))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(next.getLong(1)))
        keyOut.set(put.getRow)
        list1 += ((keyOut, put))
      }
      list1.toIterator
    })


    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "qcy:spark_country")
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[NullWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class", classOf[NullWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)
    hbaseConf.set("hbase.zookeeper.quorum","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")

    val coalesce: RDD[(ImmutableBytesWritable, Put)] = hbaseRDD.coalesce(5)

    //这个保存hbase的RDD只发起了5次的连接而不是300次连接，因为在这里给它变成了5个partition，并且没有使用shuffle的方式
    coalesce.saveAsNewAPIHadoopDataset(hbaseConf)

    println(acc.value)
  }
}
