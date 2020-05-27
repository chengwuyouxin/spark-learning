package Util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisConnectionPool extends  Serializable{
  val redisHost="22.11.97.142"
  val redisPort=6379
  val redisTimeout=30000
  val redisPassword="123456"

  lazy val pool=new JedisPool(new GenericObjectPoolConfig(),redisHost,redisPort,redisTimeout,redisPassword)

  lazy val hook=new Thread{
    @Override
    override def run(): Unit = {
      println("Execute thread hook "+this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run())

}
