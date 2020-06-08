package com.testSparkStreaming.stop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkTask {
  def createSSC(): _root_.org.apache.spark.streaming.StreamingContext = {

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = values.sum
      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)
      Some(sum + lastStatu)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")

    //设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./ck5")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))

    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)

    wordAndCount.print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck5", () => createSSC())

    new Thread(new Stop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()
  }

}
