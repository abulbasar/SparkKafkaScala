package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
/*

Use this code to generate random files. 
https://github.com/abulbasar/pyspark-examples/blob/master/random_file_generator.py

Start the file generator
$ mkdir /tmp/input
$ cd /tmp/input/
$ wget https://raw.githubusercontent.com/abulbasar/pyspark-examples/master/random_file_generator.py
$ python random_file_generator.py

*/



object StructuredFileFoeachStream {
  
  def main(args:Array[String]){
    val conf = new SparkConf()
    .setAppName(getClass.getName)
    .setIfMissing("spark.master", "local[2]")
    .setIfMissing("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    import spark.implicits._
    
    val schema = new StructType(Array(
        new StructField("value", StringType, false))
    )
    
    val source = spark.readStream.schema(schema).text("/tmp/input/*.dat")
    
    print("Is streaming: ", source.isStreaming)
    source.printSchema()
    
    val enriched = source
    .withColumn("value", expr("cast(value as double)"))
    .withColumn("outlier",  expr("value > 0.99 or value < 0.01"))
    .select("value", "outlier")
    .as[HBaseRecordType]
    
    enriched.printSchema()
    
    enriched.writeStream.format("csv").outputMode(OutputMode.Append()).start("/tmp/output")
    
    enriched.writeStream.foreach(new HBaseSink).start()
    
    enriched.writeStream.format("console").option("truncate", false).option("numRows", 10).start()
    
    
    spark.streams.awaitAnyTermination()
    spark.close()
   
  }
  
}