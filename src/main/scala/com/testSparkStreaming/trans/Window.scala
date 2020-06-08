package com.testSparkStreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByWindow").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)

//    val windowDstream: DStream[String] = socketDstream.window(Seconds(9))
    val windowDstream: DStream[String] = socketDstream.window(Seconds(9),Seconds(6))

    windowDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
