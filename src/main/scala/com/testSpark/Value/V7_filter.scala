package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V7_filter {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(List("Andy1","Bob","Cindy1","Duck"),3)
//    value.saveAsTextFile("output/filter/1")

//  过滤后，分区规则：保持原有分区，分区中有数据则保留
    val value1: RDD[String] = value.filter(_.contains('1'))
    value1.saveAsTextFile("output/filter/2")

    //4.关闭连接
    sc.stop()
  }

}
