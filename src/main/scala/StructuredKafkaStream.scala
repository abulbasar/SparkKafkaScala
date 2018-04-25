package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object StructuredKafkaStream {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")

    val spark = SparkSession.builder().config(conf).getOrCreate()


    val source = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "demo2")
      .load()

    source.printSchema()

    source
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
      .writeStream
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
    spark.close()

  }

}