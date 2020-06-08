import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Kafka_DerictAPI010Handler {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop222:9092,hadoop223:9092,hadoop224:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yang",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

//和08的区别主要在这里
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](Set(new TopicPartition("receiverApi",0)),kafkaParams,Map[TopicPartition,Long](new TopicPartition("receiverApi",0)->10)))

    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
