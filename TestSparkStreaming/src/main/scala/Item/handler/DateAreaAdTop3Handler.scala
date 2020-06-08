package Item.handler

import Item.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object DateAreaAdTop3Handler {
  /**
    *将数据存入到redis
    * 需要先转换数据结构
    *
    * @param dateAreaCityAdToCountDStream 需求二的中间结果-->每天，各个地区，各城市，各广告，点击量
    * @return
    */
  def saveToRedis(dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Long)]) = {
    //先得到需要的数据，然后转换格式
    val value: DStream[((String, String), Iterable[(String, Long)])] = dateAreaCityAdToCountDStream.map {
      case ((date, area, city, ad), count) => {
        ((date, area, ad), count)
      }
    }.reduceByKey(_ + _)
      .map {
        case ((date, area, ad), count) => {
          ((date, area), (ad, count))
        }
      }.groupByKey()
    //取top3，接下来转为需求的格式
    val top3DStream: DStream[((String, String), List[(String, Long)])] = value.mapValues(iter => {
      iter.toList.sortWith(_._2 < _._2).take(3)
    })

    //转为Json
    val dateAreaToJson: DStream[((String, String), String)] = top3DStream.mapValues(list => {
      import org.json4s.JsonDSL._
      JsonMethods.compact(list)
    })

    dateAreaToJson.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //写库操作
//        iter.foreach { case ((date, area), json) =>
//          val redisKey = s"top3_ads_per_day:$date"
//          val hashKey = area
//          //单个写入
//          jedisClient.hset(redisKey, hashKey, json)

          //要达到批次（每个分区）写入的效果：是批次所以是在foreachPartition里面写
        //到最终写库之前，数据中一定要有list用来生成map
        val dateToAreaJson: Iterator[(String, (String, String))] = iter.map {
            case ((date, area), json) => {
              (date, (area, json))
            }
          }
        val dateToDateToAreaJson: Map[String, List[(String, (String, String))]] = dateToAreaJson.toList.groupBy(_._1)

         val result: Map[String, List[(String, String)]] = dateToDateToAreaJson.mapValues(list=> list.map(_._2))

        //因为result是Map所以要foreach
        result.foreach{
          //如果result为空，不会进入这个方法，而且是foreachPartition，所以获取连接可以在foreach里写
          case (date,list)=>{
            val redisKey = s"top3_ads_per_day:$date"
            import scala.collection.JavaConversions._
            jedisClient.hmset(redisKey,list.toMap)
          }
        }

        //关闭连接
        jedisClient.close()
      })
    })
  }
}
