package com.atguigu.gmall0921.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  /*
  1 、  筛选出用户最基本的活跃行为 （ 打开应用程序 是否有start项    ，打开第一个页面 (page 项中,没有 last_page_id))
  2 、  去重，以什么字段为准进行去重 (用户id ,ip ,mid),用redis来存储已访问列表  什么数据对象来存储列表
  3、   得到用户的日活明细后， 方案一： 直接明细保存到某个数据库中   使用OLAP进行统计汇总
                             方案二： 进行统计汇总之后再保存
   */
  def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val topic ="ODS_BASE_LOG"
        val groupid="dau_app_group"
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(topic,ssc,groupid)

        //把ts 转换成日期 和小时 为后续便于处理
        val jsonObjDstream: DStream[JSONObject] = inputDstream.map{record=>
          val jsonString: String = record.value()
                val jSONObject: JSONObject = JSON.parseObject(jsonString)
               //把时间戳转换成 日期和小时字段
                val ts: lang.Long = jSONObject.getLong("ts")
                val dateHourStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
                val dateHour: Array[String] = dateHourStr.split(" ")
                val date: String = dateHour(0)
                val hour: String = dateHour(1)
                jSONObject.put("dt",date)
                jSONObject.put("hr",hour)
                jSONObject
        }
         //  map(record=>record.value)
    // 1 、  筛选出用户最基本的活跃行为 （  打开第一个页面 (page 项中,没有 last_page_id))
    val firstPageJsonObjDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
      if(pageJsonObj!=null){
        val lastPageId: String = pageJsonObj.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          true
        } else {
          false
        }
      }else{
        false
      }
    }
    firstPageJsonObjDstream.cache()
    firstPageJsonObjDstream.count().print()

    //2 、  去重，以什么字段为准进行去重 ( mid),用redis来存储已访问列表  什么数据对象来存储列表
    val dauDstream: DStream[JSONObject] = firstPageJsonObjDstream.filter { jsonObj =>
      //提取对象中的mid
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      val dt: String = jsonObj.getString("dt")
      // 查询 列表中是否有该mid
      // 设计定义 已访问设备列表
      //redis   type? string （每个mid 每天 成为一个key,极端情况下的好处：利于分布式  ）   set √ ( 把当天已访问存入 一个set key  )   list(不能去重,排除) zset(不需要排序，排除) hash（不需要两个字段，排序）
      // key? dau:[2021-01-22] (field score?)  value?  mid   expire?  24小时  读api? sadd 自带判存 写api? sadd
      val jedis = new Jedis("hdp1", 6379)
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
    }
    dauDstream.count().print()


        ssc.start()
        ssc.awaitTermination()

  }

}
