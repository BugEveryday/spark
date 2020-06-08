package com.testSparkStreaming.out

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object ForeachRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)

    val wc: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wc.foreachRDD(rdd=>{

      //获取连接

      rdd.foreachPartition(iter=>{
        iter.foreach(x=>
         //写库
          print(x))
      })
      //释放连接

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
