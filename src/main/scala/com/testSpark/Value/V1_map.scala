package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V1_map {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //    1到4,两个分区
    val rddData: RDD[Int] = sc.makeRDD(1 to 4, 2)
    //    rddData.saveAsTextFile("output/map/1")

    //    map()将RDD中的每一个数据传入到函数中
    val doubleRDDData: RDD[Int] = rddData.map(_ * 2)
    //    doubleRDDData.saveAsTextFile("output/map/2")

    //    测试直接传入一个函数
    val concatString1RDDData: RDD[String] = rddData.map(concatString1)
    concatString1RDDData.saveAsTextFile("output/map/3")
    val value: RDD[String] = rddData.map(concatString2)
//    value.saveAsTextFile("output/map/4")
    //4.关闭连接
    sc.stop()
  }

  def concatString1(i: Int) = i+"hello"

  def concatString2(i: Int) = i.toString + "hello"

}
