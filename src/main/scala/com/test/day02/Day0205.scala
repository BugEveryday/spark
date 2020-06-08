package com.test.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0205 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
//5、需求说明：创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值
    val value: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    value.glom().map(_.max).foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
