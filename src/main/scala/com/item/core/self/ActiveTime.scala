package com.item.core.self

import java.text.SimpleDateFormat

import com.item.core.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActiveTime {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

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
    //用户活跃时间段：以小时为单位，降序

    //可以自定义是哪一天的
    val day = "2019-07-17"
    value.filter(_.action_time.startsWith(day)).map(action => (action.action_time.substring(0, 13), 1L)).reduceByKey(_ + _).sortBy(_._2, false).collect().foreach(println)

    println("--------------------------------")

    //全部时间的
    value.filter(_.action_time != "null").map(action => (action.action_time.substring(11, 13), 1L)).reduceByKey(_ + _).sortBy(_._2, false).collect().foreach(println)

    println("--------------------------------")

    //自定义某段时间
    val start = "2019-07-17"
    val end = "2019-07-19"
    value.filter(_.action_time != "null").map(
      action => {
        val date = action.action_time.substring(0, 11)
        if (date >= start && date <= end) {
          (action.action_time.substring(11, 13), 1L)
        } else {
          (action.action_time.substring(11, 13), 0L)
        }
      }).reduceByKey(_ + _).sortBy(_._2, false).collect().foreach(println)

    println("--------------------------------")

    //最近N天
    //当前日期
    val nowDate :String = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis())

//    日期转换有点麻烦，不写这个了

    //4.关闭连接
    sc.stop()
  }

}
