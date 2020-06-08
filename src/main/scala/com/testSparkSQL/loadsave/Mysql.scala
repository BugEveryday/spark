package com.testSparkSQL.loadsave

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


object Mysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("mysql").master("local[*]").getOrCreate()
//    直接方法
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","000000")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop222:3306/gmall","base_region",prop)

    df.show()
//通用格式
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop222:3306/gmall")
      .option("dbtable", " base_region")
      .option("user", "root")
      .option("password", "000000")
      .load()

//    直接方法
    df.write.jdbc("jdbc:mysql://hadoop222:3306/test","base_region2",prop)
//通用格式
      df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop222:3306/gmall")
      .option("dbtable", "base_region3")
      .option("user", "root")
      .option("password", "000000")
      .save()

    spark.stop()
  }
}
