package com.testSparkSQL.tranc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建rdd
    val rdd: RDD[String] = sc.textFile("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.txt")
//创建sparksession
    val spark: SparkSession = SparkSession.builder().appName("rddToDF").master("local[*]").getOrCreate()
//将rdd转换为row类型
    val rowRDD: RDD[Row] = rdd.map(x => {
      val strings: Array[String] = x.split(",")
      Row(strings(0), strings(1).trim.toInt)
    })

    //spark创建DF，参数是Row和StructType
    val df: DataFrame = spark.createDataFrame(rowRDD, StructType(Seq(StructField("name", StringType), StructField("age", IntegerType))))

    df.show()


    //4.关闭连接
    sc.stop()
  }
}
