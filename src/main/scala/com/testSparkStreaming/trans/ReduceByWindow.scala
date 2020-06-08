package com.testSparkStreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object ReduceByWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByWindow").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)

    socketDstream.reduceByWindow((v1:String,v2:String)=>{
      v1+"->"+v2
    },
      Seconds(9),Seconds(3)).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
