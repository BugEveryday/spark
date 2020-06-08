package com.testSparkSQL.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDTF extends UserDefinedAggregateFunction{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("UDTF").master("local[*]").getOrCreate()

    spark.udf.register("myudtf",UDTF)

    val df: DataFrame = spark.read.json("E:\\Desktop\\TestSpark\\src\\main\\resources\\people.json")
    df.createOrReplaceTempView("people")

    spark.sql("select myudtf(age) from people").show()


  }

  override def inputSchema: StructType = StructType(Seq(StructField("input",LongType)))

  override def bufferSchema: StructType = StructType(Seq(StructField("sum",LongType),StructField("count",LongType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getLong(1)+1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
