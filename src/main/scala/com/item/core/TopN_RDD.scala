package com.item.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//TOPN品类，按点击、订单、支付排序  搞三种：全部RDD；使用类；使用累加器
object TopN_RDD {
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
    val orgDatasRDD: RDD[String] = sc.textFile("input/core_item1")

    val map = mutable.Map
    //    return (id-xx,1)
    orgDatasRDD.map {
      line => {
        val strings: Array[String] = line.split("_")
        //因为点击、订单和支付是互斥的，所以对于每一条数据返回的其实都只有一条，而且是相同的格式
        if (strings(6) != "-1") {
          List((strings(6) + "-click", 1))
        } else if (strings(8) != "null") {
          (strings(8) + "-order", 1)
        } else if (strings(10) != "null") {
          (strings(10) + "-pay", 1)
        } else {
          List(("null-null", 0))
        }
      }
    }



//    value.reduceByKey(_ + _).map(
//      datas => {
//        val strings: Array[String] = datas._1.split("-")
//        (strings(0), (strings(1), datas._2))
//      }.take(10)
//    ).groupByKey().foreach(println)



    //4.关闭连接
    sc.stop()
  }
}
