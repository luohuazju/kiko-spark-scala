name := "kiko-spark-scala"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.kiko.spark.trips")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1"
)