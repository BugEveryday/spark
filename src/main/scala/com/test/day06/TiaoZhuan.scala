package com.test.day06

import com.item.core.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TiaoZhuan {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
   日期       uid  sessionid                         页面id    时间         搜索     点击   订单      支付   城市
     2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
     2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
     2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:00:38_null_-1_-1_15,13,5,11,8_99,2_null_null_10
     2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_22_2019-07-17 00:00:28_null_-1_-1_null_null_15,1,20,6,4_15,88,75_9
      */

    val orgRDD: RDD[String] = sc.textFile("input/core_item1")

    val value: RDD[UserVisitAction] = orgRDD.map {
      line => {
        val strings: Array[String] = line.split("_")
        UserVisitAction(
          strings(0),
          strings(1).toLong,
          strings(2),
          strings(3).toLong,
          strings(4),
          strings(5),
          strings(6).toLong,
          strings(7).toLong,
          strings(8),
          strings(9),
          strings(10),
          strings(11),
          strings(12).toLong
        )
      }
    }

/*
页面跳转
1、先确定哪些页面
2、计算分母
3、计算分子
4、计算跳转率
 */
//    1、先确定哪些页面
    //要计算的页面id
    val pageIds = List(1,2,3,4,5,6,7)
    //页面跳转关系
    val idsRela: List[String] = pageIds.zip(pageIds.tail).map(t=>(t._1+"-"+t._2))

//    2、计算分母
    //按pageIds过滤，然后统计个数
    //    过滤到要计算的Pageid
    val theIds: RDD[UserVisitAction] = value.filter(action => pageIds.contains(action.page_id))
    //    (1,3640) (4,3602) (3,3672) (6,3593) (7,3545)  (2,3559) (5,3563)
    val idsSum: RDD[(Long, Long)] = theIds.map(action => (action.page_id, 1L)).reduceByKey(_ + _)

//    3、计算分子
    theIds.groupBy(_.session_id).mapValues(
      datas=>{
        datas.toList.sortWith(
          (l,r)=>{
            l.action_time<r.action_time
          }
        )
      }
    )




    //4.关闭连接
    sc.stop()
  }

}
