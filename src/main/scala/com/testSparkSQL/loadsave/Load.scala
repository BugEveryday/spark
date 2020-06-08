package com.testSparkSQL.loadsave

import org.apache.spark.sql.{DataFrame, SparkSession}

object Load {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("load").master("local[*]").getOrCreate()
//通用的格式
    val df: DataFrame = spark.read.format("json").load("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.json")

    df.show()

    println("-------------------")
//直接的方式
    spark.read.textFile("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.txt").show()

    spark.stop()
  }

}
