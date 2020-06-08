package com.testSpark.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KV3_groupByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[(Int, Int)] = sc.makeRDD(1 to 6).zip(sc.makeRDD(5 to 10))

    value.groupBy(t=>(t._2+t._1)%4).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)
//    (0,(0,CompactBuffer((2,6), (4,8), (6,10))))
//    (2,(2,CompactBuffer((1,5), (3,7), (5,9))))
    value.groupByKey(4).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)
    /* groupBy的最后都hash分区了
    (0,(4,CompactBuffer(8)))
    (1,(1,CompactBuffer(5)))
    (1,(5,CompactBuffer(9)))
    (2,(6,CompactBuffer(10)))
    (2,(2,CompactBuffer(6)))
    (3,(3,CompactBuffer(7)))
     */

    //4.关闭连接
    sc.stop()
  }

}
