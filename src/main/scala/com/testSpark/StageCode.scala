package com.testSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object StageCode {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2. Application：初始化一个SparkContext即生成一个Application；
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2),2)

    //3.1 聚合
    val resultRDD: RDD[(Int, Int)] = dataRDD.map((_,1)).reduceByKey(_+_)

    // Job：一个Action算子就会生成一个Job；
    //3.2 job1打印到控制台
    resultRDD.collect().foreach(println)

    //3.3 job2输出到磁盘
    resultRDD.saveAsTextFile("output")

    Thread.sleep(1000000)

    //4.关闭连接
    sc.stop()
  }

}
