package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.Dauhandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object DauApp {
  def main(args: Array[String]): Unit = {
    //1.获取SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.获取StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将数据转化为样例类并补全logdate和loghour字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH ")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //a.将数据转化为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //b.补全logdate和loghour字段
        val ts: Long = startUpLog.ts
        //c.将数据格式化 yyyy-MM-dd HH
        val dateHourStr: String = sdf.format(new Date(ts))
        //d.补全logdate yyyy-MM-dd
        startUpLog.logDate = dateHourStr.split(" ")(0)
        //e.补全loghour HH
        startUpLog.logHour = dateHourStr.split(" ")(1)

        startUpLog
      })
    })



    //5.跨批次去重
    val fileterByRedisDStream: DStream[StartUpLog] = Dauhandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

//    startUpLogDStream.cache()
//    fileterByRedisDStream.cache()
//
//    startUpLogDStream.count().print()
//    fileterByRedisDStream.count().print()

    //6.批次内去重
    val filterByMidDStream: DStream[StartUpLog] = Dauhandler.filterByMid(fileterByRedisDStream)

//    filterByMidDStream.cache()
//    filterByMidDStream.count().print()

    //7.将去重后的mid写入redis
    Dauhandler.saveMidToRedis(filterByMidDStream)

    //8.将明细数据写入HBase
    filterByMidDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL0923_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),HBaseConfiguration.create(),Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //    //打印
    //    kafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>{
    //        println(record.value())
    //      })
    //    })
    //9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
