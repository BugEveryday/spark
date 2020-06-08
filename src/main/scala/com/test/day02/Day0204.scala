package com.test.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0204 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
//4、需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    val value: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    val value1: RDD[(Int, Int)] = value.mapPartitionsWithIndex((index,datas)=>{datas.map((index,_))})
    value1.saveAsTextFile("output/test/Day0204")


    //4.关闭连接
    sc.stop()
  }

}
