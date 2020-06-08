package com.testSpark.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountByGroupBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)

    val result: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).groupBy(t => t._1).map(t => (t._1, t._2.size))
    result.foreach(println)
/*
rdd.flatMap(_.split(" ")).map((_,1)).groupBy(t=>t._1).foreach(println)
(Spark,CompactBuffer((Spark,1)))
(Hello,CompactBuffer((Hello,1), (Hello,1), (Hello,1)))
(World,CompactBuffer((World,1)))
(Scala,CompactBuffer((Scala,1)))
 */
    //4.关闭连接
    sc.stop()
  }

}
