package com.qiao.diver

import com.qiao.hbase.SparkHbaseBulkLoad
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("bulk", classOf[SparkHbaseBulkLoad], "orc转HFile")
    driver.run(args)
  }
}
