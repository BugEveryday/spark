package com.testSparkSQL.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("UDF").master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.json("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.json")

    df.createOrReplaceTempView("people")

    spark.udf.register("add",(x:String)=>"name:"+x)

    spark.sql("select add(name) from people").show()

    spark.stop()

  }

}
