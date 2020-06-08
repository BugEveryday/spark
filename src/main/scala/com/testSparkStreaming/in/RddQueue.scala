package com.testSparkStreaming.in

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object RddQueue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rddqueue").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

//    重要的是这一步，创建的是queueStream
    val queue = new mutable.Queue[RDD[Int]]
//     queueStream(queue, oneAtATime, sc.makeRDD(Seq[T](), 1))默认每次一个
//    val RDDDstream: InputDStream[Int] = ssc.queueStream(queue)
    val RDDDstream: InputDStream[Int] = ssc.queueStream(queue,false)

    RDDDstream.map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    for(i<-1 to 10){
      queue+=ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
