package com.testSpark.partitionRule

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilePartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    默认分区，核数与2的最小值，一般就是2
//    val value: RDD[String] = sc.textFile("input")
//    value.saveAsTextFile("output")
//    自定义分区，自定义为2
    val value1: RDD[String] = sc.textFile("input",2)
    value1.saveAsTextFile("output1")

    //4.关闭连接
    sc.stop()
  }

}
