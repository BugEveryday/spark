package com.testSparkStreaming.trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object UpdateStateByKeyWithoutCK {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TransWithoutState").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

//    requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint()
//    没有checkPoint无法启动，只添加checkPoint可以使用，但是挂机之后，数据不会续上

    ssc.checkpoint("./update1")

    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop222",999)
    //有状态转换的transform操作
    val wordToCount: DStream[(String, Int)] = socketDstream.flatMap(_.split(" ")).map((_,1))

    wordToCount.updateStateByKey((seq:Seq[Int],option:Option[Int])=>{
      val sum: Int = seq.sum
      val latest: Int = option.getOrElse(0)
      Some(sum+latest)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
