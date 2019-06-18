package com.qiao.kafka

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

object SparkStreamingKafkaBroadCastUpdate {
  def main(args: Array[String]): Unit = {
    val topic = "Qiao_Test"
    val brokers: String = "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"

    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingkafka").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val topicSet: Set[String] = topic.split(",").toSet
    val kafkaParams = new mutable.HashMap[String, Object]
    kafkaParams += "bootstrap.servers" -> brokers
    kafkaParams += "group.id" -> "test"
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName
    // 下面两个配置是做producer时使用的
    //    kafkaParams += "key.serializer" -> classOf[StringSerializer].getName
    //    kafkaParams += "value.serializer" -> classOf[StringSerializer].getName

    val dStreamList = new ListBuffer[InputDStream[ConsumerRecord[String, String]]]()

    for (i <- 0 until 3) {
      val partitions = new ListBuffer[TopicPartition]
      for (ii <- (8 * i) until (8 * i) + 8) {
        partitions += new TopicPartition(topic, ii)
      }
      val value: ConsumerStrategy[String, String] = ConsumerStrategies.Assign(partitions, kafkaParams)
      val lines: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, value)
      dStreamList += lines
    }

    val offset = new mutable.HashMap[TopicPartition, Long]()
    offset += new TopicPartition(topic, 0) -> 1

    val lines = streamingContext.union(dStreamList)
    val words = lines.flatMap(_.value().split(" "))

    var mapBroadcast: Broadcast[Map[String, String]] = streamingContext.sparkContext.broadcast(Map[String, String]())

    val updateInterval = 10000L
    var lastUpdateTime = 0L
    val matchAccmulator = streamingContext.sparkContext.longAccumulator
    val noMatchAccmulator = streamingContext.sparkContext.longAccumulator


    words.foreachRDD(r => {
      if (System.currentTimeMillis() - lastUpdateTime >= updateInterval) {
        val map: Map[String, String] = Map[String, String]()
        val dictpath = "data/sparkStream/contryDir/input"
        val fs = FileSystem.get(new Configuration())
        val fileStatuses = fs.listStatus(new Path(dictpath))
        //        println(fileStatuses)
        for (f <- fileStatuses) {
          val path = f.getPath
          val stream = fs.open(path)
          val reader = new BufferedReader(new InputStreamReader(stream))

          var lines = reader.readLine()
          while (lines != null) {
            val strings = lines.split("\t")
            var code = strings(0)
            var name = strings(1)
            map += code -> name
            lines = reader.readLine()
          }
        }
        // 手动取消广播变量的持久化
        mapBroadcast.unpersist()
        // 重新创建管广播变量
        mapBroadcast = streamingContext.sparkContext.broadcast(map)
        lastUpdateTime = System.currentTimeMillis()
        println(Map)
      }
      if (!r.isEmpty()) {
        r.foreachPartition(it => {
          val cast = mapBroadcast.value
          it.foreach(f => {
            println(s"${f}")
            import scala.util.control.Breaks._
            breakable {
              if (f == null) {
                break()
              }
              if (cast.contains(f)) {
                matchAccmulator.add(1L)
              } else {
                noMatchAccmulator.add(1L)
              }
            }
          })
        })
        val matchA = matchAccmulator.count
        val noMatchA = noMatchAccmulator.count

        println(s"match: ${matchA} , noMatch : ${noMatchA}")
      }

    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
