package com.qiao.util

object MyPredef {
  implicit def deleteHdfs(o:String) = new SparkWordCountDelete(o)
}
