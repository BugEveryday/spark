package com.testSpark.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
//    创建sc
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    val sc = new SparkContext(conf)

    val lineRDD:RDD[String] = sc.textFile(args(0))

    var listRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val wordToOneRDD: RDD[(String, Int)] = listRDD.map((_,1))

    val wordSumRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_+_)
    wordSumRDD.saveAsTextFile(args(1))
//    args(0)="/input"
//    args(1)="/output"
//    val tuples: Array[(String, Int)] = wordSumRDD.collect()

    sc.stop()
  }

}
