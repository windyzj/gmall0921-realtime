package com.atguigu.gmall0921.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{HbaseUtil, MykafkaSink, MykafkaUtil, OffsetManagerUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {

    //精通 cannel
    //熟练使用 canel   cannal

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")
    //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_BASE_DB_C"
    val groupid = "base_db_canal_group"

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

    //把格式转换为jsonObject 便于中间的业务处理
    val jsonObjDstream: DStream[JSONObject] = inputDstreamWithOffsetDstream.map(record=> JSON.parseObject(record.value()) )

// table  ,type ,pkNames, data-->kafka 还是 hbase
//  清单 那些表是维度表    //常量  配置文件  数据库参数
    //  事实表写-->kafka   topic ?  table     message ?  data
    //  维度表写-->hbase    hbase table?  table    rowkey?   pkNames   faimly? 常量info   column - value? data      namespace ?  常量：gmall0921
    jsonObjDstream.foreachRDD{rdd=>

      rdd.foreachPartition{jsonItr=>
        val dimTables=Array("user_info","base_province")// 维度表 用户和地区
        val factTable=Array("order_info","order_detail")// order_info 一个订单一条数据  order_detail 一个订单中每个商品一条数据
        for (jsonObj <- jsonItr ) {
          val table: String = jsonObj.getString("table")
          val optType: String = jsonObj.getString("type")
          val pkNames:JSONArray =  jsonObj.getJSONArray("pkNames")
          val dataArr:JSONArray =  jsonObj.getJSONArray("data")

          if(dimTables.contains(table) ){
            //维度处理  -->>hbase
            //  维度表写-->hbase    namespace ?  常量：gmall0921  hbase table?  dim_table DIM_USER_INFO
            //  rowkey?   pkNames   faimly?  常量info   column - value? data
            //  rowkey   唯一、region //数据分散在不同的机器上还是集中在一起，
            //  不一定。要看数据的查取场景，如果是用rowkey查询尽量打散 ，如果范围查询可以不打散
            // 如果需要打散--> 预分区   不需要打散--> 不预分区
            // 用户表使用rowkey查询 -->预分区   省市表 可以整表查询 -->不做预分区
            // 用数据的主键 1 先补0 补的位数预计是数据增上限    //再反转
            //取得数据的 主键
            val pkName: String =  pkNames.getString(0)

            import  collection.JavaConverters._
            for (data <- dataArr.asScala ) {
              val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
              println(dataJsonObj)
              val pk: String = dataJsonObj.getString(pkName)
              // 用数据的主键 1 先补0 补的位数预计是数据增上限    //再反转
              val rowkey: String = StringUtils.leftPad(pk,10,"0").reverse
              val hbaseTable:String="DIM_"+table.toUpperCase
              val dataMap: util.Map[String, AnyRef] = dataJsonObj.getInnerMap //key= columnName ,value= value

              HbaseUtil.put(hbaseTable,rowkey,dataMap)

               // 练习 把数据存入到hbase中
            }

          }
          if(factTable.contains(table)){
            //分流到事实表处理  --> kafka
            //  事实表写-->kafka   topic ?    层+table+optype   DWD_ORDER_INFO_I   message ?  data
            var opt:String=null
            if(optType.equals("INSERT")){
              opt="I"
            }else if(optType.equals("UPDATE")){
              opt="U"
            }else if(optType.equals("DELETE")){
              opt="D"
            }
           //kafka发送工具？
            val topic="DWD_"+table.toUpperCase()+"_"+opt
            //有可能一条canal的数据 有多行操作 ，要把dataArr遍历
            import  collection.JavaConverters._
            for (data <- dataArr.asScala ) {
              val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
              MykafkaSink.send(topic,dataJsonObj.toJSONString)
            }

            //也可以用for循环 如下
//            for(i <- 0 to dataArr.size()){
//              val dataJsonObj: JSONObject = dataArr.getJSONObject(i)
//              MykafkaSink.send(topic,dataJsonObj.toJSONString)
//            }

          }
        }
      }

      OffsetManagerUtil.saveOffset(topic, groupid, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
