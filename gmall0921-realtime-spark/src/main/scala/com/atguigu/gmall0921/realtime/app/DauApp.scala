package com.atguigu.gmall0921.realtime.app

import com.atguigu.gmall0921.realtime.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        //1 业务对时效的需求  2 处理业务的计算时间  尽量保证周期内可以处理完当前批次
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val topic ="ODS_BASE_LOG"
        val groupid="dau_app_group"
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(topic,ssc,groupid)
        val jsonStringDstream: DStream[String] = inputDstream.map(_.value)
         //  map(record=>record.value)

         jsonStringDstream.print(1000)

        ssc.start()
        ssc.awaitTermination()


  }

}
