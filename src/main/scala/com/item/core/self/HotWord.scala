package com.item.core.self

import com.item.core.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HotWord {
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
//    热搜
//所有用户的搜索关键词
//(吃鸡,7604) (笔记本,7407) (i7,7464) (苹果,7310) (内存,7336) (手机,3111)
    value.filter(_.search_keyword!="null").map(t=>(t.search_keyword,1L)).reduceByKey(_+_).collect().foreach(println)

    println("------------------")

// 某一天的搜索关键词
//(吃鸡,715) (笔记本,694) (i7,644) (苹果,675) (内存,674) (手机,282)
    val day="2019-07-18"
    value.filter(action=>action.search_keyword!="null" && action.action_time.startsWith(day) ).map(t=>(t.search_keyword,1L)).reduceByKey(_+_).collect().foreach(println)

//    通过filter可以查询最近24小时的热词

    //4.关闭连接
    sc.stop()
  }
}
