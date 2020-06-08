package com.testSpark.KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object KV5_foldByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a",1),("a",1),("a",1),("b",1),("b",1),("b",1),("b",1),("a",1))
    val rdd = sc.makeRDD(list,2)
//简化版的aggregateByKey，后面的两个参数（函数），省略为一个，即两个参数一样
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
