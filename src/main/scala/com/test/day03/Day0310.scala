package com.test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0310 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    10、（广告点击Top3）案例
    val value: RDD[String] = sc.textFile("input/top3")

    value.map{
      line=>{
        val strings: Array[String] = line.split(" ")
        (strings(1)+"-"+strings(4),1)
      }
    }.reduceByKey(_+_).map{
      t => {
        val strings: Array[String] = t._1.split("-")
        (strings(0), (strings(1), t._2))
      }
    }.groupByKey().mapValues(
      datas=>datas.toList.sortWith(
        (l,r)=>l._2>r._2
      ).take(3)
    ).foreach(println)

//
    //4.关闭连接
    sc.stop()
  }
}
