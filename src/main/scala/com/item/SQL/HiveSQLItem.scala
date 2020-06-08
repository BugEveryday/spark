package com.item.SQL

import com.testSparkSQL.bean.CityRatio
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object HiveSQLItem {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("itemsql").master("local[*]").enableHiveSupport().getOrCreate()

    spark.sql("select ci.area area,pi.product_name product_name,ci.city_name city_name\nfrom\n(select click_product_id,city_id from sparkpractice.user_visit_action where click_product_id>-1) ua\njoin sparkpractice.product_info pi\non ua.click_product_id=pi.product_id\njoin sparkpractice.city_info ci\non ua.city_id=ci.city_id").createOrReplaceTempView("temp")

    spark.udf.register("myudtf",new myudtf)

    spark.sql("select area, product_name,count(*) click_count,myudtf(city_name) \nfrom temp group by area,product_name").show(10,truncate = false)

    spark.stop()
  }
}
class myudtf extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Seq(StructField("city_name",StringType)))

  override def bufferSchema: StructType = StructType(Seq(StructField("buf",MapType(StringType,LongType)),StructField("total",LongType)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[String,Long]()
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val city_count: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    if(!input.isNullAt(0)){
      val city: String = input.getString(0)
      buffer(0)=city_count+(city->(city_count.getOrElse(city,0L)+1L))
    }
    buffer(1)=buffer.getLong(1)+1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    buffer1(0) = map1.foldLeft(map2) { case (map, (city, count)) => {
        map + (city -> (map.getOrElse(city, 0L) + count))
      }
    }

    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): String = {
    //取出缓冲数据
    val cityToCount: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val totalCount: Long = buffer.getLong(1)

    //计算Top2
    val top2CityCount: List[(String, Long)] = cityToCount.toList.sortWith(_._2 > _._2).take(2)

    //定义其他的占比
    var otherRatio = 1D

    //计算Top2城市占比
    val ratios: List[CityRatio] = top2CityCount.map { case (city, count) =>
      //计算当前城市占比
      val ratio: Double = count.toDouble / totalCount
      //计算其他城市占比
      otherRatio -= ratio
      CityRatio(city, ratio)
    }

    var cityRatioList: List[CityRatio]=ratios
    //添加上其他城市及占比
    if(otherRatio>0){
      cityRatioList :+= CityRatio("其他", otherRatio)
    }

    //北京21.2%，天津13.2%，其他65.6%
    cityRatioList.mkString(",")
  }
}