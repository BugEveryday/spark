package com.test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Day0307 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    7、自定义分区案例
    val value: RDD[(Int, Int)] = sc.makeRDD(1 to 6).zip(sc.makeRDD(5 to 10))

    value.partitionBy(new MPartitioner(3)).mapPartitionsWithIndex((index,datas)=>datas.map((index,_))).collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
class MPartitioner(num:Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      key.asInstanceOf[Int]*(key.asInstanceOf[Int]%num)%num
    }else{
      0
    }
  }
}