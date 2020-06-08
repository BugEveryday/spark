package com.testSpark.TwoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UnionTest {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)

//    会保留重复的数据
//    rdd1.union(rdd2).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).foreach(println)
    /*
(1,2)     (4,4)
(1,3)     (5,5)
(0,1)     (5,6)
(3,5)     (6,7)
(3,6)     (6,8)
(2,4)     (7,9)
          (7,10)
     */
    rdd1.union(rdd2).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).distinct().collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
