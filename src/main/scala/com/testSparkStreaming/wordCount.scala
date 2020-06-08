package com.testSparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)

    val wc: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wc.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
