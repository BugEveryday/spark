package com.item.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TopN_Accumulator {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val orgRDD: RDD[String] = sc.textFile("input/core_item1")

    val value: RDD[UserVisitAction] = orgRDD.map {
      line => {
        val strings: Array[String] = line.split("_")
        UserVisitAction(
          strings(0),
          strings(1).toLong,
          strings(2),
          strings(3).toLong,
          strings(4),
          strings(5),
          strings(6).toLong,
          strings(7).toLong,
          strings(8),
          strings(9),
          strings(10),
          strings(11),
          strings(12).toLong
        )
      }
    }

    val acc = new theAcc()
    sc.register(acc)

    value.foreach(
      act=>acc.add(act)
    )

    acc.value.groupBy(t=>t._1._1)

    //4.关闭连接
    sc.stop()
  }

}

class theAcc extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {
  val map = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new theAcc

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    val click = v.click_category_id.toString
    val ordered = v.order_category_ids
    val pay = v.pay_category_ids

    if (click != "-1") {
      map((click, "click")) = map.getOrElse((click, "click"),0L)+1L
    } else if (ordered != "null") {
      val strings: Array[String] = ordered.split(",")
      for (id <- strings) {
        map((id, "order")) = map.getOrElse((id, "order"),0L)+1L
      }
    } else if (pay != "null") {
      val strings: Array[String] = pay.split(",")
      for (id <- strings) {
        map((id, "pay")) = map.getOrElse((id, "pay"),0L)+1L
      }
    }
  }


  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (k,v) => {
        map(k)=map.getOrElse(k,0L)+v
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}