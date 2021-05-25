package com.kiko.spark.trips

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object Tips {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("kikoTips")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: Tips <parquet_dir>")
      sys.exit(1)
    }

    val parquetDir = args(0)

    val trips = spark.read.parquet(parquetDir)
    val tipPercentColumn = udf(calculateTipPercent)

    trips
      .withColumn("tipPercent", tipPercentColumn(trips("_c13"), trips("_c16")))
      .select("tipPercent")
      .groupBy(col("tipPercent"))
      .count()
      .orderBy(col("tipPercent"))
      .show()
  }

  /**
   * user defined function to calculate the percentage
   */
  val calculateTipPercent = (fare: Double, tip: Double) => {
    if (fare == 0) {
      "Error"
    } else {
      (tip / fare) * 100 match {
        case x if x >= 0 && x < 10 => "0-10"
        case x if x >= 10 && x < 20 => "10-20"
        case x if x >= 20 && x < 30 => "20-30"
        case x if x >= 30 && x < 40 => "30-40" // no 30 -40 in readme, I add it
        case x if x >= 40 && x < 50 => "40-50"
        case x if x >= 50 => "50+"
        case _ => "Error"
      }
    }
  }
}
