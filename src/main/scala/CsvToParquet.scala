package com.kiko.spark.trips

import org.apache.spark.sql.functions.{col, month, year}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("kikoCsvParquet")
      .getOrCreate()

    if (args.length < 2) {
      print("Usage: KikoCsvParquet <trips_csv_file> <parquet_dir>")
      sys.exit(1)
    }

    val tripsFile = args(0)
    val parquetDir = args(1)

    val trips: Dataset[Row] = spark.read.csv(tripsFile)
    trips.withColumn("year", year(col("_c4")))
      .withColumn("month", month(col("_c4")))
      .write
      .partitionBy("year", "month")
      .parquet(parquetDir)

  }

}
