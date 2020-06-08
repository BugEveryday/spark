package Item.handler

import java.text.SimpleDateFormat
import java.util

import Item.bean.Ads_log
import Item.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {
  /**
    * 将黑名单中的用户过滤掉
    *
    * @param adsLogDStream 输入的数据
    */
  def filterByBlackList(adsLogDStream: DStream[Ads_log],ssc:StreamingContext) = {
    //获取黑名单，然后判断
    //因为过滤之后还要保持原来的数据类型，所以使用的是transform，而不是foreanRDD

    //一个批次创建一个连接，将黑名单数据广播到executor
    val value: DStream[Ads_log] = adsLogDStream.transform(adsLogRDD => {

      val client: Jedis = RedisUtil.getJedisClient

      val blackList: util.Set[String] = client.smembers(blackListRedisKey)

      client.close()
      //将黑名单转为广播变量
      val blackListBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackList)
      //过滤
      adsLogRDD.filter(adsLog => !blackListBC.value.contains(adsLog.userid))
    })
    //不单独命名也可以的
    value



    //每个分区创建一个连接，但分区内只操作一次redis，将黑名单数据全部取出
//    adsLogDStream.transform(adsLogRDD=>{
//      //对每个分区进行操作
//      adsLogRDD.mapPartitions{iter=>{
//        //获取连接
//        val client: Jedis = RedisUtil.getJedisClient
//        //取出所有的黑名单
//        //每个分区创建一个连接，但分区内只操作一次redis
//        val blackList: util.Set[String] = client.smembers(blackListRedisKey)
//        //关闭连接
//        client.close()
//
//        //过滤黑名单
//        iter.filter(adsLog=>
//          !blackList.contains(adsLog.userid))
//      }}
//    })
    //如果不取出所有黑名单，而是直接iter.filter，会每个分区创建一个连接，分区内多次操作redis
    //如果直接adsLogDStream.filter，每条数据都会创建连接，操作redis

  }

  /**
    * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。并保存到redis
    *
    * @param adsLogDStream 经过黑名单过滤的数据
    */
  private val format = new SimpleDateFormat("yyyy-MM-dd")

  var blackListRedisKey = "BlackList"

  def addBlackList(adsLogDStream: DStream[Ads_log]) = {

//    转换数据，转化为【每天，某个广告，用户】，1
    val dateAdUserToOne: DStream[((String, String, String), Long)] = adsLogDStream.map(adsLog => {
      val date: String = format.format(adsLog.timestamp)
      ((date, adsLog.adid, adsLog.userid), 1L)
    })
    //求总
    val dateAdUserToCount: DStream[((String, String, String), Long)] = dateAdUserToOne.reduceByKey(_+_)

    //进行筛选，加入黑名单redis
    dateAdUserToCount.foreachRDD(rdd=>{

//      优化：使用foreachPartition，减少与redis连接的次数
      rdd.foreachPartition(iter=>{
        //创建连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //写库
        iter.foreach{case ((date,ad,user),count)=> {
          val redisKey = s"user-add$date"
          val hashKey = s"user$user-ad$ad"
          //累加Redis跟当前批次的数据
          jedisClient.hincrBy(redisKey, hashKey, count)

          //将累加之后的值获取
          if (jedisClient.hget(redisKey, hashKey).toLong >= 5000) {

            //将当前用户写入黑名单
            jedisClient.sadd(blackListRedisKey, user)
          }

        }}

        //释放连接
        jedisClient.close()

      })
    })
  }

}
