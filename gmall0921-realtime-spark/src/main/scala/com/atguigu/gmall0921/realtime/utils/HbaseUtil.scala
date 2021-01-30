package com.atguigu.gmall0921.realtime.utils

import java.util
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object HbaseUtil {

   var connection:Connection=null
  //加载配置参数
   private val properties: Properties = PropertiesUtil.load("config.properties")
   val HBASE_SERVER=properties.getProperty("hbase.server")
   val DEFAULT_FAMILY=properties.getProperty("hbase.default.family")
   val NAMESPACE=properties.getProperty("hbase.namespace")




   def put(tableName:String ,rowKey:String,columnValueMap: java.util.Map[String,AnyRef]): Unit ={
      if(connection==null) init()  //连接
      //连接 -->表
     val table: Table = connection.getTable(TableName.valueOf(NAMESPACE+":"+tableName) )
     //表--> put
    //把columnValueMap制作成一个 Put列表   一起交给table提交执行
     val puts: util.List[Put] = new java.util.ArrayList[Put]()
     import collection.JavaConverters._
     for (colValue <- columnValueMap.asScala ) {
       val col: String = colValue._1
       val value: AnyRef = colValue._2
       if(value!=null&&value.toString.length>0){
         //一个字段一个put
         val put = new Put(Bytes.toBytes(rowKey)).addColumn(Bytes.toBytes(DEFAULT_FAMILY),Bytes.toBytes(col),Bytes.toBytes(value.toString))
         puts.add(put)
       }
     }
     table.put(puts )
   }

  def init(): Unit ={
     val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum",HBASE_SERVER)
    connection=ConnectionFactory.createConnection(configuration)
  }

}
