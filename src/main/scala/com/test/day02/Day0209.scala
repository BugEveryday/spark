package com.test.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0209 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    distinct()去重源码解析（自己写算子实现去重）
    val value: RDD[Int] = sc.makeRDD(List(1,2,3,2,1,4,34,5,3))

    value.map((_, null)).reduceByKey((x, y) => x).map(_._1).foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
