package com.atguigu.gmall0921.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool: JedisPool = null

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(10) //最大连接数
      jedisPoolConfig.setMaxIdle(4) //最大空闲
      jedisPoolConfig.setMinIdle(4) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedisClient = getJedisClient
    println(jedisClient.ping())
    jedisClient.close()
  }
}

