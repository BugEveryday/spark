package com.testSparkSQL.bean

import java.text.DecimalFormat

case class CityRatio(name: String, ratio: Double) {

  private val format = new DecimalFormat("0.00%")

  override def toString: String = {
    s"$name ${format.format(ratio)}"
  }

}
