import Item.utils.RedisUtil
import redis.clients.jedis.Jedis

object TestRedis {
  def main(args: Array[String]): Unit = {
//    获取连接
    val client: Jedis = RedisUtil.getJedisClient

//    操作
    client.set("aa","bigdata")

//    释放连接
    client.close()
  }

}
