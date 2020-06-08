package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V3_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(List(1, 3, 4, 5, 6, 7), 4)
//相比于mapPartitions，多个参数-分区号。另一个参数是函数，是迭代器
//    val iterator: Iterator[Int] = List(1,2,3).iterator
//    iterator.map(_*2)
    val rdd: RDD[(Int, Int)] = value.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    rdd.saveAsTextFile("output/mapPartitionsWithIndex/1")

    //4.关闭连接
    sc.stop()
  }
}
