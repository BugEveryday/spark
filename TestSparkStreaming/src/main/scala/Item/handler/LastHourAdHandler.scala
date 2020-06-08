package Item.handler

import java.text.SimpleDateFormat

import Item.bean.Ads_log
import Item.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdHandler {

  private val format = new SimpleDateFormat("HH:mm")

  val rowkey = "last_hour_ads_click"
  /**
    *
    * @param filterAdsLogDStream 过滤后的数据
    */
  def saveToRedis(filterAdsLogDStream: DStream[Ads_log]):Unit = {

    val windowDStream: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(1))

    val adToHourCount: DStream[(String, Iterable[(String, Long)])] = windowDStream.map(adsLog => {
      val hour: String = format.format(adsLog.timestamp)
      //      各广告最近 1 小时内各分钟的点击量
      ((adsLog.adid, hour), 1L)
    }).reduceByKey(_ + _).map {
      case ((ad, hour), count) => {
        (ad, (hour, count))
      }
    }.groupByKey()

    val adToJson: DStream[(String, String)] = adToHourCount.mapValues(iter => {

      val sortedList: List[(String, Long)] = iter.toList.sortWith(_._1 < _._1)

      import org.json4s.JsonDSL._
      JsonMethods.compact(sortedList)

    })

    adToJson.foreachRDD(rdd=>{

      val client: Jedis = RedisUtil.getJedisClient

      client.del(rowkey)

      client.close()

      rdd.foreachPartition(iter=>{

        val client1: Jedis = RedisUtil.getJedisClient

        //iter本身就是可以直接toMap的
        if(iter.nonEmpty) {
          //有非空判断，所以可以把获取和关闭redis连接直接写到里面
          import scala.collection.JavaConversions._
          client1.hmset(rowkey, iter.toMap)
        }

        client1.close()

      })
    })

  }

}
