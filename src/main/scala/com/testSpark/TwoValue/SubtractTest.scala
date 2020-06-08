package com.testSpark.TwoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubtractTest {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)
    val rdd2: RDD[Int] = sc.makeRDD(5 to 10)

    rdd1.subtract(rdd2).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    rdd1.subtract(rdd2,3).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
