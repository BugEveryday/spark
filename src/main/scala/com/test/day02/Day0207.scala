package com.test.day02

import org.apache.spark.{SparkConf, SparkContext}

object Day0207 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
//7、需求说明：创建一个RDD，过滤出对2取余等于0的数据
    sc.makeRDD(List(1,2,3,4,5,6)).filter(_%2==0).saveAsTextFile("output/test/Day0207")

    //4.关闭连接
    sc.stop()
  }
}
