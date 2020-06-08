package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V10_coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD((1 to 10),10)
// shuffle为false。不落盘
    value.coalesce(5).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

//  为true，开启shuffle，落盘，解决OOM
//    减少分区，分区划分变了，每个分区数据的个数不一样
    value.coalesce(5,true).mapPartitionsWithIndex((index,datas)=>datas.map((index,_ ))).collect().foreach(println)

//    增加分区
    val value1: RDD[Int] = sc.makeRDD((1 to 10),5)
    value1.coalesce(10,true).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
