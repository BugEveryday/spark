package com.item.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TopN_CaseClass {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val orgRDD: RDD[String] = sc.textFile("input/core_item1")

    orgRDD.map {
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
    }.flatMap {
      action => {
        action match {
          case act: UserVisitAction => {
            val click = act.click_category_id.toString
            val order = act.order_category_ids
            val pay = act.pay_category_ids

            if (click != "-1") {
              List(CategoryCountInfo(click, 1, 0, 0))
            } else if (order != "null") {
              val strings: mutable.ArrayOps[String] = order.split(",")
              val list = new ListBuffer[CategoryCountInfo]
              for (i <- strings) {
                list.append(CategoryCountInfo(i, 0, 1, 0))
              }
              list
            } else if (pay != "null") {
              val strings: mutable.ArrayOps[String] = pay.split(",")
              val list = new ListBuffer[CategoryCountInfo]
              for (i <- strings) {
                list.append(CategoryCountInfo(i, 0, 0, 1))
              }
              list
            } else {
              Nil
            }
          }
          case _ => Nil
        }
      }
    }.groupBy(info=>info.categoryId).mapValues{//(4,CategoryCountInfo(4,5961,1760,1271))
      info=>{
        info.reduce{
          (v1,v2)=>{
            v1.clickCount=v1.clickCount+v2.clickCount
            v1.orderCount=v1.orderCount+v2.orderCount
            v1.payCount=v1.payCount+v2.payCount
            v1
          }
        }
      }
    }.map{
      t=>t._2
    }.sortBy(info=>(
      info.clickCount,
      info.orderCount,
      info.payCount
    ),false).take(10)

    //4.关闭连接
    sc.stop()
  }
}


//用户访问动作表
case class UserVisitAction(date: String, //用户点击行为的日期
                           user_id: Long, //用户的ID
                           session_id: String, //Session的ID
                           page_id: Long, //某个页面的ID
                           action_time: String, //动作的时间点
                           search_keyword: String, //用户搜索的关键词
                           click_category_id: Long, //某一个商品品类的ID
                           click_product_id: Long, //某一个商品的ID
                           order_category_ids: String, //一次订单中所有品类的ID集合
                           order_product_ids: String, //一次订单中所有商品的ID集合
                           pay_category_ids: String, //一次支付中所有品类的ID集合
                           pay_product_ids: String, //一次支付中所有商品的ID集合
                           city_id: Long)

//城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long)//支付次数
