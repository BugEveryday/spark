package com.testSparkStreaming.in

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object Kafka_DerictAPI08Auto {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck",
      () => {
        val conf: SparkConf = new SparkConf().setAppName("derictAuto").setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(3))

        ssc.checkpoint("./ck")

        val kafkaParams: Map[String, String] = Map[String, String](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop222:9092,hadoop223:9092,hadoop224:9092",
          ConsumerConfig.GROUP_ID_CONFIG -> "yang"
        )

        val derictDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("receiverApi"))


        derictDstream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

        ssc
      })
// 计算逻辑不能在这里，只能在getActiveorCreate方法中，不然ck中就没有计算逻辑相关记录
    ssc.start()
    ssc.awaitTermination()
  }

}
