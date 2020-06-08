package com.test.day02

import org.apache.spark.{SparkConf, SparkContext}

object Day0210 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    10、用textFile、map、flatmap、reduceByKey实现wordcount
    sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("output/test/Day0210")

    //4.关闭连接
    sc.stop()
  }
}
