package com.testSpark.Value

import org.apache.spark.{SparkConf, SparkContext}

object V11_repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD((1 to 10),5).repartition(9).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
