package com.qiao.diySort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SrcondarySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCunt").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("debug")


    import com.qiao.util.MyPredef.deleteHdfs
    val output = "data/wordCount/output/"
    output.deletePath()

    val lines = sc.textFile("data/wordCount/input" )

    val value: RDD[(SecondarySortKey, String)] = lines.flatMap(_.split(" ")).filter(f => f.length != 0).map((_, 1)).reduceByKey((_ + _),4).map(x => {
      (new SecondarySortKey(x._1, x._2), s"${x._1}\t${x._2}")
    })
    val sort = value.sortBy(_._1).map(x => s"${x._1.word}\t${x._1.count}")

    sort.saveAsTextFile(output)

  }

}
