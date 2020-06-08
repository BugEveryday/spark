package com.testSpark.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KV6_combinByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)

    val value: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (v1: (Int, Int), v2: Int) => (v1._1 + v2, v1._2 + 1),
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2))
    //(b,(286,3))   (a,(274,3))
    value.map {
      case (k: String, v: (Int, Int)) => {
        (k, v._1, v._1 / v._2.toDouble)
      }
    }.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
