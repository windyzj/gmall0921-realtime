package com.atguigu.gmall0921.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManagerUtil {



/*  读取redis中的偏移量*/
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long] ={
      val jedis: Jedis = RedisUtil.getJedisClient
      // redis  type? hash  key? topic:consumer_group     field?  partition  value? offset   expire? no  api?  hgetall
      val offsetKey=topic+":"+groupId
      val offsetMapOrigin: util.Map[String, String] = jedis.hgetAll(offsetKey)

    jedis.close()
      if(offsetMapOrigin!=null&&offsetMapOrigin.size()>0){
          import  collection.JavaConverters._
          // 转换结构把从redis中取出的结构 转换成 kafka要求的结构
          val offsetMapForKafka: Map[TopicPartition, Long] = offsetMapOrigin.asScala.map { case (partitionStr, offsetStr) =>
            val topicPartition: TopicPartition = new TopicPartition(topic, partitionStr.toInt)
            (topicPartition, offsetStr.toLong)

          }.toMap
        println("读取起始偏移量：："+offsetMapForKafka)
        offsetMapForKafka
    }else{
         null
    }




  }


  //把偏移量写入redis
  def  saveOffset(topic:String ,groupId:String,offsetRanges: Array[OffsetRange]): Unit ={
    val jedis: Jedis = RedisUtil.getJedisClient
    //把偏移量存储到redis type  hash     写入的api ?
//    key: topic+consumer_group
//    field: partition
//    value: offset
    val offsetKey=topic+":"+groupId
    //取分区和偏移量的map集合
    val offsetMapForRedis = new util.HashMap[String,String]()
    for (offsetRange <- offsetRanges ) {
      val partition: Int = offsetRange.partition  //分区
      val offset: Long = offsetRange.untilOffset  //偏移量结束点
      offsetMapForRedis.put(partition.toString,offset.toString)
    }
    //写入redis
    println("写入偏移量结束点："+offsetMapForRedis)
    jedis.hmset(offsetKey,offsetMapForRedis)
    jedis.close()
  }

}
