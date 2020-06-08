package com.test.day05

import com.item.core.{Accumu05, CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Day0502 {
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

    val accumu0 = new Accumu05
    sc.register(accumu0)
    value.foreach(accumu0.add(_))
    val value1: mutable.Map[(String, String), Long] = accumu0.value
   value1.groupBy(_._1._1).map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }.toList.sortWith(
     (l,r)=>{
       if(l.clickCount==r.clickCount){
         if(l.orderCount==r.orderCount){
             l.payCount>r.payCount
         }else{
           l.orderCount>r.orderCount
         }
       }else{
         l.clickCount>r.clickCount
       }
     }
   ).take(10).foreach(println)

    //4.关闭连接
    sc.stop()
  }
}