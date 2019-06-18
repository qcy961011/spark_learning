package com.qiao.kafka

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.actors.Actor
import scala.collection.mutable


case class KafkaObectCase(var value:Int,var name:String)

class QiaoKafkaObjectProducer(val topic:String ,val name:String) extends Actor{

  var producer: KafkaProducer[String, KafkaObectCase] = _

  def init ={
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[SerObject].getName)
    this.producer = new KafkaProducer[String, KafkaObectCase](props)
    this
  }

  override def act(): Unit = {
    var num = 1
    while (true) {
      val messageStr = new String(s"Qiao_Test_${name}_" + num)
      println("send:" + messageStr)
      this.producer.send(new ProducerRecord[String, KafkaObectCase](this.topic, new KafkaObectCase(num , num.toString)))
      num += 1
      if (num > 10) num = 0
      Thread.sleep(3000)
    }

  }
}

object QiaoKafkaObjectProducer{
  def apply(topic: String,name:String): QiaoKafkaObjectProducer = new QiaoKafkaObjectProducer(topic,name).init
}

class QiaoKafKaObjectConsumer(val topic:String,val name:String) extends Actor {

  var consumer:ConsumerConnector = _

  def init ={
    val prop = new Properties()
    prop.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    prop.put("group.id", "qiao")
    prop.put("zookeeper.session.timeout.ms", "60000")
    this.consumer = Consumer.create(new ConsumerConfig(prop))
    this
  }
  override def act(): Unit = {
    val topicCountMap = new mutable.HashMap[String,Int]()
    topicCountMap += topic -> 1
    val createMessageStreams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = createMessageStreams.get(topic).get(0)
    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator
    import scala.util.control.Breaks._
    // 在Scala中没有Continue的概念，只能通过调用breakable配合berak()方法来实现跳出
//    breakable {
      while (iterator.hasNext) {
        breakable {
          val bytes: Array[Byte] = iterator.next().message()
          if (bytes == null) {
            continue()
          }
          val oi = new ObjectInputStream(new ByteArrayInputStream(bytes))
          val obj = oi.readObject()
          val value = obj.asInstanceOf[KafkaObectCase]
          println("receive:" + value + s"_${name}")
          Thread.sleep(1)
        }
      }
//    }
  }
}

object QiaoKafKaObjectConsumer{
  def apply(topic: String,name:String): QiaoKafKaObjectConsumer = new QiaoKafKaObjectConsumer(topic,name).init
}


object KafkaObjectTest {

  def main(args: Array[String]): Unit = {
    val topic = "Qiao_Test_Object"
    val producer1 = QiaoKafkaObjectProducer(topic , "Producer_ONE")
//    val producer2 = QiaoKafkaObjectProducer(topic , "Producer_TWO")
//    val producer3 = QiaoKafkaObjectProducer(topic , "Producer_Three")
    val consumer1 = QiaoKafKaObjectConsumer(topic,"Consumer_ONE")
//    val consumer2 = QiaoKafKaObjectConsumer(topic,"Consumer_TWO")
//    val consumer3 = QiaoKafKaObjectConsumer(topic,"Consumer_Three")
    consumer1.start()
//    consumer2.start()
//    consumer3.start()
    producer1.start()
//    producer2.start()
//    producer3.start()
    //    Thread.sleep(1000)
  }
}
