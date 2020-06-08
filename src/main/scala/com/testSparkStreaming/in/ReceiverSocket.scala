package com.testSparkStreaming.in

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverSocket {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("receiver").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val receDstream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop222",999))

    receDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
