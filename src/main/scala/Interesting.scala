package com.kiko.spark.trips

import org.apache.spark.sql.functions.{col, count, format_number, hour, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Interesting {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("kikoInteresting")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: interesting <parquet_dir>")
      sys.exit(1)
    }

    val parquetDir = args(0)
    val rawTrips = spark.read.parquet(parquetDir)
    val trips2019 = rawTrips.filter("year(_c3)==2019")
    val trips2020 = rawTrips.filter("year(_c3)==2020")

    bestMoneyTime(trips2019)
    bestMoneyTime(trips2020)
  }


  val bestMoneyTime = (df: Dataset[Row]) => {
    //total_amount c21
    //pickup_datetime c3
    //id c0
    df
      .withColumn("Hour", hour(col("_c3"))) //Hour
      .select(col("_c0"), col("Hour"), col("_c21"))
      .groupBy(col("Hour")) //groupBy Hour
      .agg(sum("_c21") / count("_c0") as "RatePerRide", count("_c0") as "Count")
      .withColumn("RatePerRide", format_number(col("RatePerRide"), 2))
      .orderBy(col("Count").desc, col("RatePerRide").desc)
      .show()
  }

}
