package com.atguigu.gmall0921.realtime.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MykafkaSink {
   private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idompotence",(true: java.lang.Boolean))
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  // 轮询分区 2.x  黏性分区3.x
  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  // 根据 分区键分区
  def send(topic: String,key:String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))

  }

  def close(): Unit ={

    kafkaProducer.close()
  }

  def main(args: Array[String]): Unit = {
    send("ODS_BASE_DB_CC1","333")

    val thread101 = new Thread{
      override def  run(): Unit = {
        for(i <-0 to 100){
          Thread.sleep(100)
          println(i)
        }
      }
    }
    thread101.setDaemon(false)
    thread101.start()

    var group: ThreadGroup = Thread.currentThread.getThreadGroup
    var topGroup: ThreadGroup = group
    // 遍历线程组树，获取根线程组
    while ( {
      group != null
    }) {
      topGroup = group
      group = group.getParent
    }
    // 激活的线程数加倍
    val estimatedSize: Int = topGroup.activeCount * 2
    val slackList = new Array[Thread](estimatedSize)
    // 获取根线程组的所有线程
    val actualSize: Int = topGroup.enumerate(slackList)
    // copy into a list that is the exact size
    val list = new Array[Thread](actualSize)
    System.arraycopy(slackList, 0, list, 0, actualSize)
    System.out.println("Thread list size == " + list.length)
    for (thread <- list) {
      thread.isDaemon
      System.out.println(thread.getName+":"+thread.isDaemon)
    }
  }



}
