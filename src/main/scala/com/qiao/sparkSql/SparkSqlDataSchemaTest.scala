package com.qiao.sparkSql

import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkSqlDataSchemaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlDataSchemaTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = "data/teacherData/json/input"

    import com.qiao.util.MyPredef.deleteHdfs
    val output = "data/teacherData/json/output"
    output.deletePath()

    val map = sc.textFile(input).map(f => RowFactory.create(f))

    val fields = new ArrayBuffer[StructField]()

    fields += DataTypes.createStructField("line" , DataTypes.StringType,true)

    val tableSchema = DataTypes.createStructType(fields.toArray)

    val sqlc = new SQLContext(sc)

    val df = sqlc.createDataFrame(map , tableSchema)

    df.printSchema()
    df.show()

  }
}
