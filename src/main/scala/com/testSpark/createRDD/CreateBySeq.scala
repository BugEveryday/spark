package com.testSpark.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateBySeq {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.parallelize(Array(1,2,3,4))
    value.foreach(println)

    val value1: RDD[Int] = sc.makeRDD(Array(5,6,7,8))
    value1.foreach(println)
    val value2: RDD[Int] = sc.makeRDD(Array(1,3,5,7),3)
    value2.foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
