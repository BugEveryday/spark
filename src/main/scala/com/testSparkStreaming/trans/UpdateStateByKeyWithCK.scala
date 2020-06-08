package com.testSparkStreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UpdateStateByKeyWithCK {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./update2",
      () => {
        val conf: SparkConf = new SparkConf().setAppName("update2").setMaster("local[*]")

        val ssc = new StreamingContext(conf,Seconds(3))

      ssc.checkpoint("./update2")

     val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)

      val wordToCount: DStream[(String, Int)] = socketDstream.flatMap(_.split(" ")).map((_,1))

      wordToCount.updateStateByKey((seq:Seq[Int],option:Option[Int])=>{
        val sum: Int = seq.sum
        val latest: Int = option.getOrElse(0)
        Some(sum+latest)
      }).print()

      ssc
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
