package com.qiao.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class SparkWordCountDelete(val outPath:String) {
  def deletePath() ={
    val hadoopConf = new Configuration()

    val fs = FileSystem.get(hadoopConf)

    val path = new Path(outPath)

    if (fs.exists(path)) {
      fs.delete(path,true)
    }
  }
}
