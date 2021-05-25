package com.kiko.spark.trips

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Usage: Count <data_path>
 */
object Count {
  def main(args: Array[String]): Unit = {
    //Init spark session
    val spark = SparkSession
      .builder()
      .appName("kikoCount")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: Count <data_path>")
      sys.exit(1)
    }

    val dataPath = args(0)
    val trips: Dataset[Row] = spark.read.csv(dataPath)
    val count = trips.count()
    print("count is " + count)
  }
}
