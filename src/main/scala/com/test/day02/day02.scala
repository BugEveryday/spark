package com.test.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object day02 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(Array("a","b","c","d"),2)
    value.mapPartitions(_.map("".concat(_))).saveAsTextFile("output/test/day02")
    //4.关闭连接
    sc.stop()
  }

}
