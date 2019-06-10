package com.qiao.ex

import com.qiao.util.{ORCFormat, ORCUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcStruct}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

object Distrinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Distrinct").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()

    val data = sc.newAPIHadoopFile("data/orcData/input",
      classOf[OrcNewInputFormat],
      classOf[NullWritable],
      classOf[OrcStruct],
      hadoopConf)

    val setData = data.mapPartitions(f => {
      var dis = Map[String,Int]()

      val orcUtil = new ORCUtil
      orcUtil.setORCtype(ORCFormat.INS_STATUS)

      f.foreach(ff => {
        orcUtil.setRecord(ff._2)
        val pkgnameCode: String = orcUtil.getData("pkgname")
        dis += (pkgnameCode ->1)
      })
      dis.toIterator
    })

    val map: Map[String, Int] = setData.collect().toMap

    map.foreach( x => {
      println(x)
    })
  }
}
