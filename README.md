# kiko-spark-scala
spark scala latest version

Count

```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Count ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/csv/trips > ./output/count.out 2> ./output/count.err
```

Tips
```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Tips ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/parquet/trips > ./output/tips.out 2> ./output/tips.err
```

Interesting
```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Interesting ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/parquet/trips > ./output/interesting.out 2> ./output/interesting.err
```