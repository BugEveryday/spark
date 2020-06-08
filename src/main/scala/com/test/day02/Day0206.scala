package com.test.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0206 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
//6、需求说明：创建一个RDD，按照元素模以2的值进行分组。
    val value: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    value.groupBy(_%2).saveAsTextFile("output/test/Day0206")

    //4.关闭连接
    sc.stop()
  }
}
