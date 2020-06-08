package com.test.day02

import org.apache.spark.{SparkConf, SparkContext}

object Day0208 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    8、需求说明：创建一个RDD（1-10），从中选择放回和不放回抽样
    sc.makeRDD(1 to 10).sample(false,0.5).foreach(println)
    sc.makeRDD(1 to 10).sample(true,2).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
