package com.testSparkSQL.loadsave

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Save {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("save").master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.json("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.json")
//通用格式
    df.write.mode(SaveMode.Ignore).format("json").save("SparkSQL-save/json")

    df.write.mode(SaveMode.Ignore).format("csv").save("SparkSQL-save/csv")
//直接方法
//    df.write.text("SparkSQL-save/text")
//org.apache.spark.sql.AnalysisException: Text data source supports only a single column, and you have 2 columns.
    df.write.json("SparkSQL-save/dirc_json")

    spark.stop()
  }

}
