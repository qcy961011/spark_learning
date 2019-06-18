package com.qiao.kafka.self

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

object OfficlalKafkaSpark {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OfficlalKafkaSpark").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val topic = "Qiao_Test"
    val brokers:String="s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "qiao",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val partition = new TopicPartition(topic, 1)
    val partitions = new ListBuffer[TopicPartition]()
    partitions += partition
    val value: ConsumerStrategy[String, String] = ConsumerStrategies.Assign(partitions, kafkaParams)

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      value
    )
    val data: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    data.foreachRDD(r => {
      r.foreach(f => {
        println(f)
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
