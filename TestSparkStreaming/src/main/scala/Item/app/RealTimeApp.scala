package Item.app

import Item.bean.Ads_log
import Item.handler.{AdClickCountHandler, BlackListHandler, DateAreaAdTop3Handler, LastHourAdHandler}
import Item.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
// 业务层，只负责接收数据，调用方法完成业务需求
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("realTime").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)

//    测试是否可用
//    kafkaDStream.map(_.value()).print()

    //将从Kafka读出的数据转换为样例类对象，方便下一步处理
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(record => {
      val value: String = record.value()
      val arr: Array[String] = value.split(" ")
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    //2.0 因为用到了updateStateByKey，需要设置checkpoint
    //但是不能使用ssc.checkpoint，因为会将逻辑也写入，需要序列化，这样的话1.1传入的ssc就不能使用了，所以采用下面的做法
    ssc.sparkContext.setCheckpointDir("./realTime")


//    需求一：实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑，黑名单保存到redis
    //获取黑名单，不能写到这里----》这个位置在main线程，只会运行一次，黑名单数据不会更新
    //1.1 让数据经过黑名单的过滤
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream,ssc)

    //1.2 将满足黑名单条件的用户添加到黑名单，并保存到redis
    BlackListHandler.addBlackList(adsLogDStream)

//    需求二：广告点击量实时统计    实时统计每天各地区各城市各广告的点击流量，并将其存入Redis
    //2.1 获得每天各地区各城市各广告的点击流量
    val dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Long)] = AdClickCountHandler.getDateAreaCityAdToCount(filterAdsLogDStream)

    //2.2 将数据存入到redis
    AdClickCountHandler.saveToRedis(dateAreaCityAdToCountDStream)


//    需求三：每天各地区热门广告点击量    每天各地区 top3 热门广告：Redis数据格式为json
    //需求二中的2.1已经把 每天 各地区 各城市 各广告 的 点击流量算好了，所以直接传入这个值
    //3 将数据存入到redis
    DateAreaAdTop3Handler.saveToRedis(dateAreaCityAdToCountDStream)


//    需求四：统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量 Redis存储数据结构为json
    LastHourAdHandler.saveToRedis(filterAdsLogDStream)


    ssc.start()
    ssc.awaitTermination()
  }

}
