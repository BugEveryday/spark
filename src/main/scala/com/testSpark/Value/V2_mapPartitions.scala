package com.testSpark.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object V2_mapPartitions {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 100, 3)

    //    val mapStart: Long = System.currentTimeMillis()
    //    val mapRDD: RDD[Any] = value.map(_*2)
    //    mapRDD.collect()
    //    val mapTime=System.currentTimeMillis()-mapStart

    //    val mapPartitionsStart=System.currentTimeMillis()
//    每次传入的是某个分区的全部数据，一个分区只调用一次函数
//    因此，要在函数中进行运算，需要将某个分区的数据一个个取出来
//    但是这里的_是不一样的，第一个是迭代器，第二个是迭代器中的一个数据
//    val mapPartitionRDD: RDD[Any] = value.mapPartitions((datas)=>{datas.map(_ * 2)})
//    val iterator: Iterator[Int] = List(1,2,3).iterator
//    iterator.map(_*2)
    val mapPartitionRDD: RDD[Any] = value.mapPartitions(_.map(_ * 2))
    mapPartitionRDD.saveAsTextFile("output/mapPartition/1")
    //    mapPartitionRDD.collect()
    //    val mapPartitionsTime=System.currentTimeMillis()-mapPartitionsStart

    //    println(mapTime+"==="+mapPartitionsTime)//707===80
    //4.关闭连接
    sc.stop()
  }

}
