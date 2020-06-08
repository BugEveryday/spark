package com.test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Day0309 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    9.创建一个pairRDD，根据key计算每种key的均值
val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)
    input.combineByKey(
      (_,1),
      (t:(Int,Int),v:Int)=>(t._1+v,t._2+1),
      (t1:(Int,Int),t2:(Int,Int))=>(t1._1+t2._1,t1._2+t2._2)).map{
      case (k:String,t:(Int,Int))=>(k,t._1,t._1/t._2)}.mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
