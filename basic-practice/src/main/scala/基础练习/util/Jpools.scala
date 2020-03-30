package 基础练习.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
 * description
 *
 * @author 漩涡鸣人 2019/10/30 11:11
 */
object Jpools {
    private val poolconfig =new GenericObjectPoolConfig()
    poolconfig.setMaxIdle(5)//最大空闲连接数   连接池中的最大空闲连接数 默认是 8
    poolconfig.setMaxTotal(2000)// 只支持最大连接数  连接池中的最大连接数  默认是 8

    private  val jedisPool = new  JedisPool("127.0.0.1")

    def getJedis={
      val jedis = jedisPool.getResource
      jedis.select(12)
      jedis
    }
  }