package com.testSparkStreaming.trans

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByWindow").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    //    这个是第2种，有invReduce，需要ck
    ssc.checkpoint("./reduceByKeyAndWindow")

    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222", 999)

    val lineDstream: DStream[(String, Int)] = socketDstream.flatMap(_.split(" ")).map((_, 1))

    //    这个是第一种，没有invReduce，不需要ck
    //    lineDstream.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Seconds(9),Seconds(6)).print()
    lineDstream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      (v1: Int, v2: Int) => v1 - v2,
      Seconds(9),
      Seconds(6),
      new HashPartitioner(2),
      (x:(String,Int)) => x._2 > 0
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
