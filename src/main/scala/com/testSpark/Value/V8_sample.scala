package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V8_sample {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 10)
//    val value1: RDD[Int] = value.sample(false,0.5)
//    value1.foreach(println)
    value.sample(true,3).foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
