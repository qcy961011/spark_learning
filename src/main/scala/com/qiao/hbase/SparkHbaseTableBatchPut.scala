package com.qiao.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkHbaseTableBatchPut {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHbaseTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val value = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    println(value.getNumPartitions)

    value.foreachPartition(f => {
      val hbaseConf= HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val table = connection.getTable(TableName.valueOf("qcy:spark_country"))

      val list = f.toList
      val puts = new ListBuffer[Put]
      for (next <- list){
        val put = new Put(Bytes.toBytes(s"spark_batch_${next}"))
        put.addColumn(Bytes.toBytes("cf1") , Bytes.toBytes("count") , Bytes.toBytes(next))
        puts += put
      }
      import scala.collection.convert.wrapAsJava._
      table.put(puts)
      table.close()
      connection.close()

    })
  }
}
