package com.atguigu.gmall0921.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0921.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0921.realtime.utils.{HbaseUtil, MykafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OrderWideApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("order_wide_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val orderInfoTopic = "DWD_ORDER_INFO_I"
    val orderDetailTopic = "DWD_ORDER_DETAIL_I"

    val groupid = "order_wide_group"

    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic, groupid)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic, groupid)

    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = null

    //如果能取得偏移量则从偏移量位置取得数据量 否则 从最新的位置取得数据流
    if (orderInfoOffsetMap == null) {
      orderInfoInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, groupid)
    } else {
      orderInfoInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMap, groupid)
    }

    if (orderDetailOffsetMap == null) {
      orderDetailInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, groupid)
    } else {
      orderDetailInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMap, groupid)
    }

    // 3获得偏移量结束点  OffsetRange[]
    //主表
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoInputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      orderInfoOffsetRanges = hasOffsetRanges.offsetRanges
      rdd
    }
    //明细表
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailInputDstreamWithOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      orderDetailOffsetRanges = hasOffsetRanges.offsetRanges
      rdd
    }

    //把流转换成便于处理的格式
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputDstreamWithOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val create_time: String = orderInfo.create_time
      val creatTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = creatTimeArr(0)
      orderInfo.create_hour = creatTimeArr(1).split(":")(0)
      orderInfo
    }
    val orderDetailDstream: DStream[OrderDetail]=orderDetailInputDstreamWithOffsetDstream.map{record=>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }
    //用userid查询用户信息
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoDstream.map { orderInfo =>

      val rowKey: String = HbaseUtil.getDimRowkey(orderInfo.user_id.toString)
      val userInfoJsonObj: JSONObject = HbaseUtil.get("DIM_USER_INFO", rowKey)

      val date: Date = userInfoJsonObj.getDate("birthday")
      val userBirthMills: Long = date.getTime
      val curMills = System.currentTimeMillis()
      orderInfo.user_age = ((curMills - userBirthMills) / 1000 / 60 / 60 / 24 / 365).toInt
      orderInfo.user_gender = userInfoJsonObj.getString("gender")
      orderInfo
    }
   // 合并省市的信息
//    orderInfoWithUserDstream.mapPartitions(orderInfoItr=>
    ////      //查询 省市列表
    ////
    ////      for (orderInfo <- orderInfoItr ) {
    ////        //用orderInfo的province_id跟省市去匹配
    ////      }
    ////
    ////    )
    // 合并省市的信息
    //用driver查询hbase  通过广播变量发放到各个executor
     //  provinceMap 得到省市集合 HbaseUtil.getx
    // province数据是绝对不会变的
    // driver 中 ，  执行一次 ， 只有启动任务时 ，不是没有每个周期
    // 如果省市会发生变化  如何调整？？？？？？？？？？？？
/*    val provinceMap: mutable.Map[String, JSONObject] = HbaseUtil.scanTable("DIM_BASE_PROVINCE")
    //封装入广播变量
    val provinceBC: Broadcast[mutable.Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
    orderInfoWithUserDstream.map {orderInfo=>
      //展开广播变量
      val provinceMap: mutable.Map[String, JSONObject] = provinceBC.value
      val provinceObj: JSONObject = provinceMap.getOrElse(   HbaseUtil.getDimRowkey(orderInfo.province_id.toString) ,null )
      orderInfo.province_name=provinceObj.getString("name")

    }*/
     //transform 提供一个在driver中周期性执行的 代码位置
    val orderInfoWithDimDstream: DStream[OrderInfo] = orderInfoWithUserDstream.transform { rdd =>
      //driver   // 每个批次执行一次
      val provinceMap: mutable.Map[String, JSONObject] = HbaseUtil.scanTable("DIM_BASE_PROVINCE")
      //封装入广播变量
      val provinceBC: Broadcast[mutable.Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
      val orderInfoRDD: RDD[OrderInfo] = rdd.map { orderInfo => //executor执行
        //展开广播变量
        val provinceMap: mutable.Map[String, JSONObject] = provinceBC.value
        val provinceObj: JSONObject = provinceMap.getOrElse(HbaseUtil.getDimRowkey(orderInfo.province_id.toString), null)
        orderInfo.province_name = provinceObj.getString("name") //  Sugar
        orderInfo.province_area_code = provinceObj.getString("area_code") //省市行政区域码 dataV
        orderInfo.province_iso_code = provinceObj.getString("iso_code") // 国际编码（旧) superSet
        orderInfo.province_3166_2_code = provinceObj.getString("iso_3166_2") // 国际编码（新) kibana
        orderInfo
      }
      orderInfoRDD
    }

    //流join  1 把流改为k-v tuple2结构 2 进行join操作  得到合并的元组
    val orderInfoWithIdDstream: DStream[(Long, OrderInfo)] = orderInfoWithDimDstream.map(orderInfo=>(orderInfo.id,orderInfo))

    val orderDetailWithIdDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
   //shuffle
    val orderJoinDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithIdDstream.join(orderDetailWithIdDstream)

//  能不能写一个通用的解决方案  把缓存处理封装起来
    //    def     DstreamJoinUtil(Dstream[k,v1], Dstream[k,v2]) :  Dstream[(k,(v1,v2))]

    val orderFullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDstream.fullOuterJoin(orderDetailWithIdDstream)

    val orderWideDStream: DStream[OrderWide] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          //1  都不等于None 说明左右两边成功匹配   匹配成功 组成一条宽表数据
          // 增加了一个宽表类 OrderWide=OrderInfo+OrderDetail
          val orderWide = new OrderWide(orderInfo, orderDetail)
          orderWideList.append(orderWide)
        }
        //确定主表不等于None
        //2.1  把主表的数据写入缓存
        //设计redis ?   type?  string      key?   ORDER_INFO:+order_id     field(score) value? orderInfoJson       expire ? 600秒    api 读写 ? get +setex
        // 场景是从表通过order_id 查询主表信息
        val orderInfoKey = "ORDER_INFO:" + orderInfo.id
        //可以使用专业json4s  scala json转换工具
        val orderInfoJson = JSON.toJSONString(orderInfo, new SerializeConfig(true))
        jedis.setex(orderInfoKey, 600, orderInfoJson)
        //2.2  尝试读取从表的缓存 和自己匹配     如果匹配成功  组成一条宽表数据
        val orderDetailKey = "ORDER_DETAIL:" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailSet != null && orderDetailSet.size() > 0) {
          import collection.JavaConverters._
          for (orderDetailJson <- orderDetailSet.asScala) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //组成一条宽表数据
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }
        }
      } else { //如果主表为None ， 从表一定不为None
        //3.1  把从表的数据写入缓存  orderDetail
        // 场景：主表通过order_id查从表缓存
        val orderDetail: OrderDetail = orderDetailOption.get
        // 设计redis ？ type ?   set    key ?  ORDER_DETAIL:+ order_id      value?  orderDetailJsons   expire?  600  api?  sadd    smembers
        val orderDetailKey = "ORDER_DETAIL:" + orderDetail.order_id
        val orderDetailJson = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)
        //3.2  尝试读取主表的缓存 和自己匹配     如果匹配成功  组成一条宽表数据
        val orderInfoKey = "ORDER_INFO:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length() > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          val orderWide = new OrderWide(orderInfo, orderDetail)
          orderWideList.append(orderWide)
        }
      }


      jedis.close()
      orderWideList
    }


    orderWideDStream.print(1000)




   // orderInfoWithDimDstream.print(1000)
    //orderDetailDstream.print(1000)

    ssc.start()
    ssc.awaitTermination()





  }

}
