package com.qiao.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class SparkHbaseBulkLoad
object SparkHbaseBulkLoad {

  def main(args: Array[String]): Unit = {

    import com.qiao.util.MyPredef.deleteHdfs
    val hdfsOutPath = "data/orcData/output/"
//    val hdfsOutPath = args(0)

    hdfsOutPath.deletePath()

    val orcPath = "data/orcData/input"
//    val orcPath = args(1)


    val conf = new SparkConf().setAppName("SparkHbaseBulkLoad")
    conf.setMaster("local[*]")
    conf.set("spark.serializer" , classOf[KryoSerializer].getName)
    val sc = new SparkContext(conf)

    val hivec = new HiveContext(sc)
    val df = hivec.read.orc(orcPath)
    val hbaseRdd = df.limit(10).rdd.mapPartitions(rows => {
      val list = rows.toList
      val list1 = new ListBuffer[(ImmutableBytesWritable, KeyValue)]()

      for (row <- list) {
        val rk = new ImmutableBytesWritable()
        rk.set(Bytes.toBytes(s"spark_bulk_0620_${row.getString(1)}"))
        val keyValue = new KeyValue(rk.get(), Bytes.toBytes("cf1"), Bytes.toBytes("country"), Bytes.toBytes(4))
        list1 += ((rk, keyValue))
      }
      list1.toIterator
    }).sortByKey()

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    val job = Job.getInstance(hbaseConf)
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val table = connection.getTable(TableName.valueOf("qcy:spark_country")).asInstanceOf[HTable]
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)
    hbaseRdd.saveAsNewAPIHadoopFile(hdfsOutPath , classOf[ImmutableBytesWritable] , classOf[KeyValue] , classOf[HFileOutputFormat2] , hbaseConf)

    val loader = new LoadIncrementalHFiles(hbaseConf)
    val admin: Admin = connection.getAdmin
    loader.doBulkLoad(new Path(hdfsOutPath),admin,table,table.getRegionLocator())

  }
}
