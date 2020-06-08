package com.testSparkStreaming.in

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka_DerictAPI08Handler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("derictHandler")

    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop222:9092,hadoop223:9092,hadoop224:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yang"
    )
//获取上一次启动最后保留的Offset=>getOffset(MySQL)
    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition,Long](TopicAndPartition("receiverApi",0)->30)
//相比于自动，这里多了fromOffsets
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams, fromOffsets, (m: MessageAndMetadata[String, String]) => m.message())

    //6.创建一个数组用于存放当前消费数据的offset信息
    var offsetRanges = Array.empty[OffsetRange]

    //7.获取当前消费数据的offset信息
    val wordToCountDStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    //8.打印Offset信息
    wordToCountDStream.foreachRDD(rdd => {
      for (o <- offsetRanges) {
        println(s"${o.topic}:${o.partition}:${o.fromOffset}:${o.untilOffset}")
      }
      rdd.foreach(println)
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
