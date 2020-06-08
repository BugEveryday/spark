package com.testSparkSQL.loadsave

import org.apache.spark.sql.{DataFrame, SparkSession}

object Parquet {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("parquet").master("local[*]").getOrCreate()
//默认的就是parquet格式，所以没有format时，加载和保存的都是parquet
    val df: DataFrame = spark.read.load("E:/Desktop/TestSpark/src/main/resources/users.parquet")

    df.show()

    df.write.save("SparkSQL-save/parquet")

    spark.stop()
  }

}
