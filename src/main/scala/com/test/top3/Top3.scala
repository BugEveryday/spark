package com.test.top3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    /*
    统计每个省份广告点击top3
      时间戳        省份    城市         用户       广告
      20190212      辽宁    沈阳         张三        AAA
      20190214      河北    唐山         王五       AAA
      输出：河北 (AAA,200)(BBB,100)(CCC,20)
     */
    val value: RDD[String] = sc.textFile("input/top3")

    //    统计广告点击，以 省份-广告 为key
    val key: RDD[(String, Int)] = value.map(
      line => {
        val strings: Array[String] = line.split(" ")
        (strings(1) + "-" + strings(4), 1)
      }
    )

    //    统计广告点击，计数
    val keySum: RDD[(String, Int)] = key.reduceByKey(_ + _)
    //  (0-0,15)    (3-10,17)    (0-26,24)

    //  转换为 省份,(广告,点击)
    val provAdsClick: RDD[(String, (String, Int))] = keySum.map(
      t => {
        val strings: Array[String] = t._1.split("-")
        (strings(0), (strings(1), t._2))
      }
    )
    //(7,(18,17))    (4,(20,11))    (1,(10,12))

    //
    val value1: RDD[(String, Iterable[(String, Int)])] = provAdsClick.groupByKey()

    val tuples: RDD[(String, List[(String, Int)])] = value1.mapValues {
      datas=>{
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    }
    tuples.foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
