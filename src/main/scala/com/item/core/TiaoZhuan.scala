package com.item.core

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

    //需要统计的页面id
    val ids = List(1, 2, 3, 4, 5, 6, 7)
    //跳转关系
    val idRela: List[String] = ids.zip(ids.tail).map(t => {
      t._1 + "-" + t._2
    })

    //先算分母，即每个页面的访问次数
    //只算需要统计的页面，即先把页面id过滤一下
    val pageSum: Map[Long, Long] = value.filter(action => ids.init.contains(action.page_id)).map(action => {
      (action.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap //(4,3602)    (6,3593)    (2,3559)    (1,3640)    (3,3672)    (5,3563)

    //计算分子，即跳转的访问次数
    //把页面跳转id拼一下,跳转是个人行为，以sessionid分组，然后按时间升序排序，然后获取页面id，最后拼接，过滤获取跳转id
    val real: RDD[List[String]] = value.groupBy(_.session_id).mapValues(
      datas => {
        val ids: List[Long] = datas.toList.sortWith(
          (l, r) => {
            l.action_time < r.action_time
          }
        ).map(_.page_id)
        val realIds: List[String] = ids.zip(ids.tail).map {
          case (pageId1, pageId2) => {
            pageId1 + "-" + pageId2
          }
        }
        realIds.filter(id => idRela.contains(id))
      }
    ).map(_._2)
    val idsSum: RDD[(String, Long)] = real.flatMap(
      datas => {
        datas.map((_, 1L))
      }
    ).reduceByKey(_ + _)//(3-4,62)    (5-6,52)    (6-7,69)    (4-5,66)    (1-2,55)    (2-3,71)

    idsSum.foreach(
      t=>{
        val strings: Array[String] = t._1.split("-")
       println((strings(0), t._2.toDouble/pageSum.getOrElse(strings(0).toLong,1L)))
      }
    )
/*
(3,0.016884531590413945)
(5,0.014594442885209093)
(6,0.0192040077929307)
(4,0.018323153803442533)
(1,0.01510989010989011)
(2,0.019949423995504357)
 */

    //4.关闭连接
    sc.stop()
  }
}
