package com.testSpark.KeyValue

import com.testSpark.TwoValue.MyPartitioner
import org.apache.spark.{SparkConf, SparkContext}

object KV2_reduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    rdd.reduceByKey(_*_).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)

    rdd.reduceByKey(new MyPartitioner(3),_+_).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

}
