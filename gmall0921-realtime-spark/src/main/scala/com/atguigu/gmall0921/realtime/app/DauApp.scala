package com.atguigu.gmall0921.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.bean.DauInfo
import com.atguigu.gmall0921.realtime.utils.{MyEsUtil, MykafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


//使用手动后置提交偏移量     做幂等性存储处理

object DauApp {

  /*
  1 、  筛选出用户最基本的活跃行为 （ 打开应用程序 是否有start项    ，打开第一个页面 (page 项中,没有 last_page_id))
  2 、  去重，以什么字段为准进行去重 (用户id ,ip ,mid),用redis来存储已访问列表  什么数据对象来存储列表
  3、   得到用户的日活明细后， 方案一： 直接明细保存到某个数据库中   使用OLAP进行统计汇总
                             方案二： 进行统计汇总之后再保存
   */

  /*
    手动后置提交偏移量
    1读取偏移量初始值
        从redis中读取偏移量的数据，
            偏移量在redis中以什么样的格式保存
            主题-消费者组-分区-offset
    2加载数据
       如果能取得偏移量则从偏移量位置取得数据量 否则 从最新的位置取得数据流

    3获得偏移量结束点
     4存储偏移量
  */

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_LOG"
    val groupid = "dau_app_group"

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupid)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    //如果能取得偏移量则从偏移量位置取得数据量 否则 从最新的位置取得数据流
    if (offsetMap == null) {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, groupid)
    } else {
      inputDstream = MykafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupid)
    }
    // 3获得偏移量结束点  OffsetRange[]
    var offsetRanges: Array[OffsetRange] = null //driver ? executor? dr  以分区数为长度的数组

    //从流中顺手牵羊把本批次的偏移量结束点存入全局变量中。 ConsumerRecord[K, V]  K是kafka的分区键 V是值
    val inputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      // driver ? executor? dr //在dr中周期性 执行  可以写在 transform中，或者从rdd中提取数据比如偏移量
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      offsetRanges = hasOffsetRanges.offsetRanges
      //            rdd.map{a=> //driver ? executor? ex
      //            }
      rdd
    }


    //把ts 转换成日期 和小时 为后续便于处理
    val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      //把时间戳转换成 日期和小时字段
      val ts: lang.Long = jSONObject.getLong("ts")
      val dateHourStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = dateHourStr.split(" ")
      val date: String = dateHour(0)
      val hour: String = dateHour(1)
      jSONObject.put("dt", date)
      jSONObject.put("hr", hour)
      jSONObject
    }
    //  map(record=>record.value)
    // 1 、  筛选出用户最基本的活跃行为 （  打开第一个页面 (page 项中,没有 last_page_id))
    val firstPageJsonObjDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
      if (pageJsonObj != null) {
        val lastPageId: String = pageJsonObj.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }
    firstPageJsonObjDstream.cache() //
    firstPageJsonObjDstream.count().print()
    // val jedis = RedisUtil.getJedisClient //driver  定义变量 ex中可以使用 前提1 该对象必须可以序列化 2 不能改值
    //2 、  去重，以什么字段为准进行去重 ( mid),用redis来存储已访问列表  什么数据对象来存储列表
    /* val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.filter { jsonObj =>
       //提取对象中的mid
       val mid: String = jsonObj.getJSONObject("common").getString("mid")
       val dt: String = jsonObj.getString("dt")
       // 查询 列表中是否有该mid
       // 设计定义 已访问设备列表
       val jedis = RedisUtil.getJedisClient
       //redis   type? string （每个mid 每天 成为一个key,极端情况下的好处：利于分布式  ）   set √ ( 把当天已访问存入 一个set key  )   list(不能去重,排除) zset(不需要排序，排除) hash（不需要两个字段，排序）
       // key? dau:[2021-01-22] (field score?)  value?  mid   expire?  24小时  读api? sadd 自带判存 写api? sadd
       val key = "dau:" + dt
       val isNew: lang.Long = jedis.sadd(key, mid)
       jedis.expire(key, 3600 * 24)
       jedis.close()

       // 如果有 过滤掉该对象  如果没有保留 //插入到该列表中
       if (isNew == 1L) {
         true
       } else {
         false
       }
     }*/

    //优化过 ： 优化目的 减少创建（获取）连接的次数 ，做成每批次每分区 执行一次
    val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.mapPartitions { jsonObjItr =>
      val jedis = RedisUtil.getJedisClient //该批次 该分区 执行一次
    val filteredList: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjItr) { //条为单位处理
        //提取对象中的mid
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dt: String = jsonObj.getString("dt")
        // 查询 列表中是否有该mid
        // 设计定义 已访问设备列表

        //redis   type? string （每个mid 每天 成为一个key,极端情况下的好处：利于分布式  ）   set √ ( 把当天已访问存入 一个set key  )   list(不能去重,排除) zset(不需要排序，排除) hash（不需要两个字段，排序）
        // key? dau:[2021-01-22] (field score?)  value?  mid   expire?  24小时  读api? sadd 自带判存 写api? sadd
        val key = "dau:" + dt
        val isNew: lang.Long = jedis.sadd(key, mid)
        jedis.expire(key, 3600 * 24)

        // 如果有(非新)放弃    如果没有 (新的)保留 //插入到该列表中
        if (isNew == 1L) {
          filteredList.append(jsonObj)
        }
      }
      jedis.close()
      filteredList.toIterator
    }

    // dauDstream.count().print()

    dauDstream.foreachRDD { rdd =>
      //      rdd.foreach{jsonObj=>
      //        println(jsonObj)
      //      }

      rdd.foreachPartition { jsonObjItr =>
        //存储jsonObjItr 整体  //dr? ex? ex
        //jsonObjItr  作为整体进行保存

        //精确一次 消费要实现   后置提交偏移量+ 幂等性保存

        //批量保存//jsonObjItr 一起保存到容器中
        val docList: List[JSONObject] = jsonObjItr.toList
        if (docList.size > 0) {
          //
          //uuid //mechineid+时间戳
          val dateFormat = new SimpleDateFormat("yyyyMMdd")
          val dt: String = dateFormat.format(new Date()) //取当前日期
          //幂等性保存?  如何做到幂等性保存？给每条数据提供一个id
          // 选择id  1 ：必须保证 该id 在索引中是唯一  2： 必须保证id在不同时间 不同情况下 提交是不会变化
          val docWithIdList: List[(String, DauInfo)] = docList.map { jsonObj =>
            val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
            val mid: String = jsonObj.getJSONObject("common").getString("mid")
            val uid: String = jsonObj.getJSONObject("common").getString("uid")
            val ar: String = jsonObj.getJSONObject("common").getString("ar")
            val ch: String = jsonObj.getJSONObject("common").getString("ch")
            val vc: String = jsonObj.getJSONObject("common").getString("vc")
            val dt: String = jsonObj.getString("dt")
            val hr: String = jsonObj.getString("hr")
            val ts: Long = jsonObj.getLong("ts")
            val dauInfo=DauInfo(mid,uid,ar,ch,vc,dt,hr,ts)
            (mid, dauInfo)
          }
          //批量保存数据
          MyEsUtil.saveBulk("gmall0921_dau_info_" + dt, docWithIdList)
        }
        //driver ? ex?   ex 每批次 每分区
        //  OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)//1
      }
      //driver ? ex? d 周期性执行
      //2在driver存储driver全局数据 ，每个批次保存一次偏移量
      OffsetManagerUtil.saveOffset(topic, groupid, offsetRanges)

      println("AAAA")
    }
    //driver ? ex? d 只执行一次
    //OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)//3
    println("BBBB") // 在控制台中先打印A还是先打印B

    ssc.start()
    ssc.awaitTermination()

  }

}

// 1 理解： 能够是到 每行都是干什么用的  用到的时候 知道把哪行 粘到 你的类中什么地方
// 2 微调： 要会调试  比如要把偏移量保存在mysql  你能不能改？