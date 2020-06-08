package Item.handler

import java.text.SimpleDateFormat

import Item.bean.Ads_log
import Item.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AdClickCountHandler {
  /**
    * 将数据存入到Redis
    * @param dateAreaCityAdToCountDStream 每天各地区各城市各广告的点击流量
    */
  def saveToRedis(dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Long)]) = {
    /*
    用hash存
    date是rowkey
    area，city，ad是hashkey
    count是value
     */
    dateAreaCityAdToCountDStream.foreachRDD(dateAreaCityAdToCountRDD=>{
      dateAreaCityAdToCountRDD.foreachPartition(iter=>{
        //获取连接
        val client: Jedis = RedisUtil.getJedisClient

        //redis操作的准备工作
        val dateAreaCityAdToCountList: List[((String, String, String, String), Long)] = iter.toList

        val dateToAreaCityAdToCount: Map[String, List[((String, String, String, String), Long)]] = dateAreaCityAdToCountList.groupBy(_._1._1)

        val stringToTuples: Map[String, List[(String, String)]] = dateToAreaCityAdToCount.mapValues(list => {
          list.map { case ((date, area, city, ad), count) =>
            (s"$area-$city-$ad", count.toString)
          }
        })
        stringToTuples.foreach{
          case(date,list)=>{
            val rowkey = s"date-area-city-ad-$date"
            import scala.collection.JavaConversions._
            //redis操作，批量主要就是体现在这里，hmset，而hmset需要util.Map
            client.hmset(rowkey,list.toMap)
          }
        }


        //关闭连接
        client.close()
      })
    })

  }

  /**
    * 将经黑名单过滤的数据，转换格式为（（date，area，city，ad），1L），并计算广告点击量（全局）
    *
    * @param filterAdsLogDStream 过滤后的数据
    */
  private val format = new SimpleDateFormat("yyyy-MM-dd")

  def getDateAreaCityAdToCount(filterAdsLogDStream: DStream[Ads_log]) = {

    filterAdsLogDStream.map(adsLogRDD=>{
      val date: String = format.format(adsLogRDD.timestamp)
      ((date,adsLogRDD.area,adsLogRDD.city,adsLogRDD.adid),1L)
    })
      .updateStateByKey((seq:Seq[Long],option:Option[Long])=>{
        val sum: Long = seq.sum
        val lastSum:Long=option.getOrElse(0L)
        Some(sum+lastSum)
    })
  }



}
