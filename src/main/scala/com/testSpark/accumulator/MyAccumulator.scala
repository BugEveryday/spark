package com.testSpark.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MyAccumulator {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    val my = new MyAccumulator()
    sc.register(my)

    dataRDD.foreach(
      t=>my.add(t)
    )
    println(my.value)//Map(a1 -> 2, a0 -> 2)

    //4.关闭连接
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[(String, Int), mutable.Map[String, Int]] {
  val map = mutable.Map[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = new MyAccumulator

  override def reset(): Unit = map.clear()

  //  List(("a", 1), ("a", 2), ("a", 3), ("a", 4))
  override def add(v: (String, Int)): Unit = {
    if (v._2 % 2 == 0) {
      map("%s0".format(v._1))= v._2
    } else {
      map("%s1".format(v._1))= v._2
    }
  }

  override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
    other.value.foreach {
      case (k, v) => {
        map(k) = map.getOrElse(k, 0) + 1
      }
    }
  }

  override def value: mutable.Map[String, Int] = map
}