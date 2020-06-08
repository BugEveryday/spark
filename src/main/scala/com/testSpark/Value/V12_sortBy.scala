package com.testSpark.Value

import org.apache.spark.{SparkConf, SparkContext}

object V12_sortBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(List(2, 1, 3, 4, 6, 5),6).sortBy(_%6).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
