package com.test.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(Array("12","234","234","2345"),2)

    value.aggregate("0")((x,y)=>Math.max(x.length,y.length).toString,(a,b)=>a+b)

    //4.关闭连接
    sc.stop()
  }
}
