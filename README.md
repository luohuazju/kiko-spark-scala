# kiko-spark-scala
spark scala latest version

```
sbt clean package
```

Count

```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Count ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/csv/trips > ./output/count.out 2> ./output/count.err
```

Submit to Standalone Cluster
```
spark-submit --name "spark-kiko-count" --driver-memory 1g --class com.kiko.spark.trips.Count --master spark://centos7-master:7077 ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /opt/spark_data/csv/trips
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

Meaning
```
>. ~/.zshrc
>spark-submit --driver-memory 2g --class com.kiko.spark.trips.Meaning ./target/scala-2.12/kiko-spark-scala_2.12-0.1.jar /Users/carl/spark_data/csv/trips > ./output/meaning.out 2> ./output/meaning.err
```