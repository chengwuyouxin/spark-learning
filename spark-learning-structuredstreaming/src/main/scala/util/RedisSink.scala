package util

import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class RedisSink extends ForeachWriter[Row]{
  var jedis:Jedis=null
  val redisHost="22.11.97.142"
  val redisPort=6379
  val redisTimeout=30000
  val redisPassword="123456"
  override def open(partitionId: Long, version: Long): Boolean = {
    val config=new JedisPoolConfig()
    config.setMaxTotal(20) //通过pool获得的最大连接个数
    config.setMaxIdle(5)  //最多空闲连接数
    config.setMaxWaitMillis(1000) //阻塞时间超过1秒会报错
    val jedisPool=new JedisPool(config,redisHost,redisPort,redisTimeout,redisPassword)
    jedis=jedisPool.getResource()
    return true
  }

  override def process(value: Row): Unit = {
    jedis.hset(value.get(1).toString,value.get(0).toString,value.get(2).toString)
  }

  override def close(errorOrNull: Throwable): Unit = {
    jedis.close()
  }
}
