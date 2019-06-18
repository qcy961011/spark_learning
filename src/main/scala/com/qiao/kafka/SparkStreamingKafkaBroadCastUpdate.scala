package com.qiao.kafka

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Map}


object SparkStreamingKafkaBroadCastUpdateLingGe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingKafkaBroadCastUpdate").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf,Durations.seconds(5))
    val topic:String="wangliang11"
    val brokers:String="s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"
    val kafkaParams = new HashMap[String,Object]()
    kafkaParams+="bootstrap.servers"->brokers
    kafkaParams+="group.id"->"test"
    kafkaParams+="key.deserializer"->classOf[StringDeserializer].getName
    kafkaParams+="value.deserializer"->classOf[StringDeserializer].getName
    val buffer = new ListBuffer[InputDStream[ConsumerRecord[String,String]]]
    for(index<-0 until 3){
      val partitions=new ListBuffer[TopicPartition]
      for(partition<- (8*index) until(8*index)+8){
          partitions+=new TopicPartition(topic,partition)
      }
      val assign: ConsumerStrategy[String, String] = ConsumerStrategies.Assign(partitions,kafkaParams)
      val lines: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,assign)
      buffer+=lines
    }
    val unionResult: DStream[ConsumerRecord[String, String]] = streamingContext.union(buffer)
    val words: DStream[String] = unionResult.flatMap(_.value().split(" "))
    var lastUpdateTime=0L
    val updateInterval=10000L
    val hasCountry = streamingContext.sparkContext.longAccumulator
    val noCountrky =streamingContext.sparkContext.longAccumulator
    var mapBroad: Broadcast[Map[String, String]] = streamingContext.sparkContext.broadcast(Map[String,String]())
    words.foreachRDD(f=>{
      if(mapBroad.value.isEmpty||System.currentTimeMillis()-lastUpdateTime>=updateInterval){
        val map = Map[String,String]()
        val file="data/sparkStream/contryDir/input"
        val fs = FileSystem.get(new Configuration())
        val filelist: Array[FileStatus] = fs.listStatus(new Path(file))
        for(fi<-filelist){
          val filepath: Path = fi.getPath
          val fileinputstream: FSDataInputStream = fs.open(filepath)
          val reader = new BufferedReader(new InputStreamReader(fileinputstream))
          var lines: String = reader.readLine()
          while(lines !=null){
            val strings = lines.split("\t")
            val code=strings(0)
            val countryName=strings(1)
            map+=code->countryName
            lines = reader.readLine()
          }
        }
        mapBroad.unpersist()
        mapBroad = streamingContext.sparkContext.broadcast(map)
        lastUpdateTime=System.currentTimeMillis()
      }
      println(mapBroad.value)
      if(!f.isEmpty()){
        f.foreachPartition(
          ff=>{
            val counrty_dict: mutable.Map[String, String] = mapBroad.value
            ff.foreach(it=>{
              println(s"${it}")
              import scala.util.control.Breaks._
              breakable{
                if(it==null){
                  break()
                }
                if(counrty_dict.contains(it)){
                  hasCountry.add(1L)
                }else{
                  noCountrky.add(1L)
                }
              }
            }
            )
          }
        )
        val matchA: Long = hasCountry.count
        val noMatchA:Long =noCountrky.count
        println(s"has country:${matchA};noCountry:${noMatchA}")
        hasCountry.reset()
        noCountrky.reset()
      }

    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
