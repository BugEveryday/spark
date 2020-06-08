package com.testSparkStreaming.in

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka_ReceiverAPI {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("receiverApi").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val receDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
      "hadoop222:2181,hadoop223:2181,hadoop224:2181",
      "yang",
      Map[String, Int]("receiverApi" -> 1))

    receDstream.map{case(key,value)=>{
      (value,1)
    }}.reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
