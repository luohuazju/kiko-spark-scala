package com.kiko.spark.trips

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import java.sql.Timestamp

object Meaning {
  def main(args: Array[String]) {
    // Init spark session
    val spark = SparkSession
      .builder
      .appName("Meaning")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: meaning <parquet_dir>")
      sys.exit(1)
    }

    // get command line args
    val tripsFile = args(0)

    // YOUR CODE GOES HERE
    implicit val encRaw: Encoder[RawTrip] = Encoders.product[RawTrip]
    val schema = Encoders.product[RawTrip].schema
    val trips = spark
      .read
      .schema(schema)
      .option("timestampFormat", "MM/dd/yyyy HH:mm:ss.SSSZZ")
      .csv(tripsFile)
      .as[RawTrip]
      .limit(100)
    trips.createOrReplaceTempView("rawtrips")

    implicit val encTrip: Encoder[Trip] = Encoders.product[Trip]
    val tripsDF = spark.sql(
      "SELECT _c0 as id, _c3 as pickupDate, _c4 as dropoffDate, _c13 as fare, _c16 as tip, _c21 as total FROM rawtrips where _c13 > 5")
      .as[Trip]
    tripsDF.show()
  }

  case class Trip(
                   id: Int,
                   pickupDate: Timestamp,
                   dropoffDate: Timestamp,
                   fare: BigDecimal,
                   tip: BigDecimal,
                   total: BigDecimal
                 )

  case class RawTrip(
                      _c0: Int,
                      _c1: Int,
                      _c2: String,
                      _c3: Timestamp,
                      _c4: Timestamp,
                      _c5: String,
                      _c6: Int,
                      _c7: BigDecimal,
                      _c8: BigDecimal,
                      _c9: BigDecimal,
                      _c10: BigDecimal,
                      _c11: Int,
                      _c12: BigDecimal,
                      _c13: BigDecimal,
                      _c14: BigDecimal,
                      _c15: BigDecimal,
                      _c16: BigDecimal,
                      _c17: BigDecimal,
                      _c18: BigDecimal,
                      _c19: BigDecimal,
                      _c20: BigDecimal,
                      _c21: BigDecimal,
                      _c22: String,
                      _c23: Int,
                      _c24: Int,
                      _c25: Int,
                      _c26: Int,
                      _c27: Int
                    )
}
