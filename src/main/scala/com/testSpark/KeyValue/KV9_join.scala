package com.testSpark.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KV9_join {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
     val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 2), ("b", 1)))

    //3.2 创建第二个pairRDD
   val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("a", 3), ("b", 2), ("b", 3)))

    rdd.join(rdd1).collect().foreach(print)//(a,(1,3))(a,(2,3))(b,(1,2))(b,(1,3))

    //以第一个rdd为基准，对rdd1进行匹配

    //4.关闭连接
    sc.stop()
  }

}
