package com.atguigu.gmall0921.realtime.bootstrap

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.atguigu.gmall0921.realtime.utils.{MykafkaSink, MysqlUtil, PropertiesUtil}

object DimInit {

// 引导程序 把历史数据引入到kafka中
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("diminit.properties")
    val tableNames: String = properties.getProperty("bootstrap.tablenames")
    val topic =properties.getProperty("bootstrap.topic")
    val tableNameArr: Array[String] = tableNames.split(",")
    for (tableName <- tableNameArr ) {
      // 读取mysql
      val dataObjList: util.List[JSONObject] = MysqlUtil.queryList("select * from "+tableName)
      // 写入kafka
     //模仿canal数据的格式 去构造message
      val messageJSONobj=new JSONObject()
      messageJSONobj.put("data",dataObjList)

      messageJSONobj.put("database","gmall0921")

      val pkNames = new JSONArray
      pkNames.add("id")
      messageJSONobj.put("pkNames",pkNames)
      messageJSONobj.put("table",tableName)
      messageJSONobj.put("type","INSERT")
      println(messageJSONobj)
      MykafkaSink.send( topic , messageJSONobj.toString) //能不能直接把数据写入 kafka

    }

   // 为什么一定要close // close 执行flush 把在子线程batch中的数据 强制写入kafka
    //否则 在主线程关闭时会结束掉所有守护线程，而kafka的producer就是守护线程 ，会被结束掉有可能会丢失数据。
   MykafkaSink.close()
  }

}
