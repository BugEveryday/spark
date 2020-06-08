package com.testSpark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SysAccumulator {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    val sum: LongAccumulator = sc.longAccumulator("sum")

    val accumulator: LongAccumulator = sc.longAccumulator

    dataRDD.foreach(
      data=>accumulator.add(data._2)
    )
//    println(sum)//LongAccumulator(id: 0, name: Some(sum), value: 10)
//    println(sum.value)
//  也就是说，可以不用给累加器命名，直接就能用
    println(accumulator)//LongAccumulator(id: 1, name: None, value: 10)

    //4.关闭连接
    sc.stop()
  }
}
