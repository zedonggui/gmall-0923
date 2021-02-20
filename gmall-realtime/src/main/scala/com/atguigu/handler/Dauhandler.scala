package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object Dauhandler {
  /**
    * 将去重后的mid写入redis
    *
    * @param startUpLogDStream
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        //1.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        partition.foreach(log => {
          //2.写库
          val redisKey = "DAU:" + log.logDate
          jedisClient.sadd(redisKey, log.mid)
        })
        //3.归还连接
        jedisClient.close()
      })
    })
  }
}
