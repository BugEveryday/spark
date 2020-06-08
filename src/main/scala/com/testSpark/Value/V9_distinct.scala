package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V9_distinct {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Char] = sc.makeRDD(List('A','B','C','C','C','D'))

//    value.distinct().foreach(println)
    //    按照值的hashCode%分区数来划分数据,默认的是最大核数
    value.distinct().saveAsTextFile("output/distinct/2")
//底层 map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

//    按照值的hashCode%分区数来划分数据
//    value.distinct(3).saveAsTextFile("output/distinct/1")

    //4.关闭连接
    sc.stop()
  }

}
