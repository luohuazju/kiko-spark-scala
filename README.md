# kiko-spark-scala
spark scala latest version

```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Count ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/parquet/trips > ./output/count.out 2> ./output/count.err
```