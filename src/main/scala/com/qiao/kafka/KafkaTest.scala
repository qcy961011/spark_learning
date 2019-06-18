package com.qiao.kafka

import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.actors.Actor
import scala.collection.mutable

class QiaoKafkaProducer(val topic: String, val name: String) extends Actor {

  var producer: KafkaProducer[String, String] = _

  def init: QiaoKafkaProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    this.producer = new KafkaProducer[String, String](props)
    this
  }

  override def act(): Unit = {
    var num = 1
    while (true) {
      val messageStr = new String(s"Qiao_Test_${name}_" + num)
      println("send:" + messageStr)
      this.producer.send(new ProducerRecord[String, String](this.topic, messageStr))
      num += 1
      if (num > 10) num = 0
      Thread.sleep(3000)
    }

  }
}

object QiaoKafkaProducer {
  def apply(topic: String, name: String): QiaoKafkaProducer = new QiaoKafkaProducer(topic, name).init
}

class QiaoKafKaConsumer(val topic: String, val name: String) extends Actor {

  var consumer: ConsumerConnector = _

  def init = {
    val prop = new Properties()
    prop.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    prop.put("group.id", "qiao")
    prop.put("zookeeper.session.timeout.ms", "60000")
    this.consumer = Consumer.create(new ConsumerConfig(prop))
    this
  }

  override def act(): Unit = {
    val topicCountMap = new mutable.HashMap[String, Int]()
    topicCountMap += topic -> 1
    val createMessageStreams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = createMessageStreams.get(topic).get(0)
    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator
    while (iterator.hasNext) {
      println("receive:" + new String(iterator.next.message) + s"_${name}")
      Thread.sleep(1)
    }
  }
}

object QiaoKafKaConsumer {
  def apply(topic: String, name: String): QiaoKafKaConsumer = new QiaoKafKaConsumer(topic, name).init
}


object KafkaTest {

  def main(args: Array[String]): Unit = {
    val topic = "Qiao_Test"
    val producer1 = QiaoKafkaProducer(topic, "Producer_ONE")
    val producer2 = QiaoKafkaProducer(topic, "Producer_TWO")
    val producer3 = QiaoKafkaProducer(topic, "Producer_Three")
    val consumer1 = QiaoKafKaConsumer(topic, "Consumer_ONE")
    val consumer2 = QiaoKafKaConsumer(topic, "Consumer_TWO")
    val consumer3 = QiaoKafKaConsumer(topic, "Consumer_Three")
    consumer1.start()
    consumer2.start()
    consumer3.start()
    producer1.start()
    producer2.start()
    producer3.start()
    //    Thread.sleep(1000)
  }
}
