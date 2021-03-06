package com.testSpark.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KV10_cogroup {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))

    //3.2 创建第二个RDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6),(5,7)))

    rdd.cogroup(rdd1).collect().foreach(println)
//    (4,(CompactBuffer(d),CompactBuffer()))
//    (1,(CompactBuffer(a),CompactBuffer(4)))
//    (5,(CompactBuffer(),CompactBuffer(7)))
//    (2,(CompactBuffer(b),CompactBuffer(5)))
//    (3,(CompactBuffer(c),CompactBuffer(6)))

//4.关闭连接
    sc.stop()
  }
}
