package com.testSpark.TwoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object IntersectionTest {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)//0=>1 ;1=>2,3 ;2=>4 ;3=>5,6
    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)//0=>4 ;1=>5,6 ;2=>7,8;3=>9,10
//    rdd1.mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)
//    rdd2.mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).foreach(println)
//    rdd1.intersection(rdd2).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)//(0,4) (1,5) (2,6)
//
//    rdd1.intersection(rdd2,3).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).foreach(println)//(0,6) (1,4) (2,5)

    rdd1.intersection(rdd2,new MyPartitioner(5)).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      (key.asInstanceOf[Int]+2*partitions)%partitions
    }else{
      0
    }
  }
}