package com.testSparkStreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransWithoutState {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TransWithoutState").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)
//无状态转换的transform操作
    val wordCount: DStream[(String, Int)] = socketDstream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
