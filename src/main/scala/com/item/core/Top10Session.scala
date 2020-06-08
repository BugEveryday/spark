package com.item.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Top10Session {
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
    val top10Infos: List[CategoryCountInfo] = value1.groupBy(_._1._1).map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }.toList.sortWith(
      (l, r) => {
        if (l.clickCount == r.clickCount) {
          if (l.orderCount == r.orderCount) {
            l.payCount > r.payCount
          } else {
            l.orderCount > r.orderCount
          }
        } else {
          l.clickCount > r.clickCount
        }
      }
    ).take(10)
//    需求2：Top10热门品类中每个品类的Top10活跃Session统计
    /*
CategoryCountInfo(15,6120,1672,1259)
CategoryCountInfo(2,6119,1767,1196)
CategoryCountInfo(20,6098,1776,1244)
CategoryCountInfo(12,6095,1740,1218)
CategoryCountInfo(11,6093,1781,1202)
CategoryCountInfo(17,6079,1752,1231)
CategoryCountInfo(7,6074,1796,1252)
CategoryCountInfo(9,6045,1736,1230)
CategoryCountInfo(19,6044,1722,1158)
CategoryCountInfo(13,6036,1781,1161)
     */
//    把top10给取出来
    val top10Ids: List[String] = top10Infos.map(_.categoryId)

//    把top10id广播出去，方便操作
    val idList: Broadcast[List[String]] = sc.broadcast(top10Ids)
//    idList.value.foreach(println)
//    把id对应的session取出来
    value.filter {
      action => {
        if (action.click_category_id != -1) {
          idList.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    }. map(//转换成（品类id--sessionid，1）
      action=>{
        (action.click_category_id+"--"+action.session_id,1)
      }
    ).reduceByKey(_+_).map(
      t=>{
        val strings: Array[String] = t._1.split("--")
        (strings(0),(strings(1),t._2))
      }
    ).groupByKey().mapValues(
      datas=>{
        datas.toList.sortWith(
          (l,r)=>{
            l._2>r._2
          }
        ).take(3)
      }
    ).foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
class Accumu05 extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]] {
  val map=mutable.Map[(String,String),Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new Accumu05

  override def reset(): Unit = map.clear()

  override def add(v: UserVisitAction): Unit = {
    val click = v.click_category_id.toString
    val order = v.order_category_ids
    val pay = v.pay_category_ids
    if(click!="-1"){
      map((click,"click"))=map.getOrElse((click,"click"),0L)+1L
    }else if(order!="null"){
      val strings: Array[String] = order.split(",")
      for(id<-strings){
        map((id,"order"))=map.getOrElse((id,"order"),0L)+1L
      }
    }else if(pay!="null"){
      val strings: Array[String] = pay.split(",")
      for(id<-strings){
        map((id,"pay"))=map.getOrElse((id,"pay"),0L)+1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (k, v) => {
        map(k)=map.getOrElse(k,0L)+v
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}
