package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._


object StructuredKafkaStream {
  
  def main(args:Array[String]){
    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[2]")
    .setIfMissing("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
   
    
    val source = spark.readStream
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "demo")
		  .load()
		
		source.printSchema();
	
		val consoleSink = source
		.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
		.writeStream
		.format("console")
		.start()
    
    spark.streams.awaitAnyTermination()
    spark.close()
   
  }
  
}