package com.testSparkSQL.tranc

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkSQLTest").master("local[*]").getOrCreate()

    //导入隐式转换
    import spark.implicits._

    val df: DataFrame = spark.read.json("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.json")
    //dsl风格
    df.filter($"age">21).show()

    println("--------------------------------------")

    //sql风格
    df.createOrReplaceTempView("people")

    spark.sql("select * from people").show()

    spark.stop()
  }
}
