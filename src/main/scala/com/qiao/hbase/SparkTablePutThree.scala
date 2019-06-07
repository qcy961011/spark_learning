package com.qiao.hbase

/**
  *
  */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkTablePutThree {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkTablePutThree").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),1)
    val tableName = "hly:country_status"
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,classOf[TableOutputFormat[NullWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class", classOf[NullWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)
    hbaseConf.set("hbase.zookeeper.quorum","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")

    val value: RDD[(NullWritable, Put)] = unit.map(t => {
      val put = new Put(Bytes.toBytes(s"spark_tab_${t}"))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("count"), Bytes.toBytes(t))
      (NullWritable.get(), put)
    })
    value.saveAsNewAPIHadoopDataset(hbaseConf)
  }

}
