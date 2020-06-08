package com.item.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TopN_CaseClass_reduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val orgRDD: RDD[String] = sc.textFile("input/core_item1")

    orgRDD.map(
      line => {
        val strings: Array[String] = line.split("_")
        UserVisitAction2(
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
    ).flatMap(
      action => {
        action match {
          case act: UserVisitAction2 => {
            val click = act.click_category_id.toString
            val order = act.order_category_ids
            val pay = act.pay_category_ids
            if (click != "-1") {
              List((click,CategoryCountInfo2(click, 1, 0, 0)))
            } else if (order != "null") {
              val strings: Array[String] = order.split(",")
              val list = new ListBuffer[(String,CategoryCountInfo2)]
              for (id <- strings) {
                list.append((id,CategoryCountInfo2(id, 0, 1, 0)))
              }
              list
            } else if (pay != "null") {
              val strings: Array[String] = pay.split(",")
              val list = new ListBuffer[(String,CategoryCountInfo2)]
              for (id <- strings) {
                list.append((id,CategoryCountInfo2(id, 0, 0, 1)))
              }
              list
            } else {
              Nil
            }
          }
          case _=> Nil
        }
      }
    ).reduceByKey(
      (info1,info2)=>{
        info1.clickCount=info1.clickCount+info2.clickCount
        info1.orderCount=info1.orderCount+info2.orderCount
        info1.payCount=info1.payCount+info2.payCount
        info1
      }
    ).map(_._2).sortBy(info2=>(info2.clickCount,info2.orderCount,info2.payCount),false).take(10)


    //4.关闭连接
    sc.stop()
  }

}

//用户访问动作表
case class UserVisitAction2(date: String, //用户点击行为的日期
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
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo2(categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long)//支付次数