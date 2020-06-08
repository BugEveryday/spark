package com.testSparkSQL.loadsave

import org.apache.spark.sql.SparkSession

object Hive {
  def main(args: Array[String]): Unit = {
    val spark1: SparkSession = SparkSession.builder().appName("hive1").master("local[*]").enableHiveSupport().getOrCreate()
//    spark1.sql("create table hive1(id int)")
    spark1.sql("show tables").show()

    spark1.sql("select\n    create_date,\n    retention_day,\n    count(*) retention_count\nfrom gmall.dws_user_retention_day\nwhere dt='2019-02-11' \ngroup by create_date,retention_day").show()

    val spark2: SparkSession = SparkSession.builder().appName("hive2").master("local[*]").getOrCreate()
    spark2.sql("show tables").show()

    spark1.stop()
    spark2.stop()

  }
}
