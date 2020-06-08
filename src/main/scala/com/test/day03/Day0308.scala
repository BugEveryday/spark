package com.test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0308 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    8.取出每个分区相同key对应值的最大值，然后相加
val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    rdd.aggregateByKey(0)(math.max(_,_),_+_).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
