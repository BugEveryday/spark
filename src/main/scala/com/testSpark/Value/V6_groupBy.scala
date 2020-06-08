package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V6_groupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(List("Andy","Bob","Cindy","Duck"),3)

//    value.saveAsTextFile("output/groupby/1")

//    value.groupBy(_.charAt(0)).saveAsTextFile("output/groupby/2")
//    value.groupBy(s=>s).saveAsTextFile("output/groupby/3")

    println("Duck".hashCode())
    //Andy  2045346   0
    //Bob   66965     2
    //Cindy 65112893  2
    //Duck  2141401   1

//groupBy的分区数就是makeRDD时传入的分区数
    val value1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    value1.saveAsTextFile("output/groupby/4.1")
    value1.groupBy(i=>i).saveAsTextFile("output/groupby/4.2")


    //4.关闭连接
    sc.stop()
  }

}
