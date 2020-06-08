package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V4_flatMap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Array[Int]] = sc.makeRDD(List(Array(1,2),Array(3,4),Array(5,6),Array(7,8)),3)

//    val value1: RDD[Int] = value.flatMap((s:Array[Int])=>s.map(_ *2))
    val value1: RDD[Int] = value.flatMap(s=>s.map(_ *1))
//    value1.saveAsTextFile("output/flatMap/1")
//    value1.foreach(println)
//    val ints: Array[Int] = Array(1,2).map(_*2)
//    val ints1: List[Int] = List(1,2).map(_*2)

    val value2: RDD[Array[List[String]]] = sc.makeRDD(List(Array(List("Alice","Andy"),List("Jack","Jeery")), Array(List("Bob")), Array(List("Candy")), Array(List("Dock"))), 3)
//    value2.flatMap(_.toList).collect().map(_.foreach(println))
    value2.flatMap(_.toList).saveAsTextFile("output/flatMap/2")

//    Array可以toList转为List
//    val list: List[List[String]] = Array(List("Alice","Andy"),List("Jack","Jeery")).toList

//    value2.map(_.length).collect().foreach(println)
    /*
    value2.flatMap(list=>list).collect().foreach(println)
    List(1, 2)
    List(3, 4)
    List(5, 6)
    List(7, 8)
     */

    //4.关闭连接
    sc.stop()
  }

}
