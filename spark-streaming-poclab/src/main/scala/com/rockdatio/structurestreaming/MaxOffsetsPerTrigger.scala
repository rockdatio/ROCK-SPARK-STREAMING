package com.rockdatio.structurestreaming

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class MaxOffsetsPerTrigger extends Serializable {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")
  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("streaming Test")

  @transient lazy val ss: SparkSession = SparkSession
    .builder()
    .appName("streaming Test")
    .config(conf)
    .getOrCreate()
  val sc: SparkContext = ss.sparkContext
  //    import ss.implicits._

  def start(): Unit = {
    val inputTopic = "rawbadi2"

    val maxtrigger = 5000

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("maxOffsetsPerTrigger", maxtrigger.toLong)
      .option("startingOffsets", "earliest")
      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("failOnDataLoss", "false")
      .option("subscribe", "rawbadi")
      .load()


    // JSON FUNCTION DESERIALIZATION
    val topicSchema = StructType(
      Array(
        StructField("card_number", StringType),
        StructField("transaction_type", StringType),
        StructField("transaction_date", StringType),
        StructField("phone_number", StringType),
        StructField("amount", StringType),
        StructField("headerId", StringType)
      ))
    //  DEBUG MODE, IN CONSOLE.
    val query: StreamingQuery = kafkaDF
      .select(col("value").cast(StringType).alias("json"))
      .select(from_json(col("json"), topicSchema) as "data")
      .select("data.*")
      .writeStream
      .partitionBy("transaction_type")
      .outputMode("append")
      .format("parquet") // supports these formats : csv, json, orc, parquet
      .option("path", s"src/resources/datalkeSS/${inputTopic}/transactions")
      .option("checkpointLocation", s"src/resources/datalkeSS/${inputTopic}/checkpoint/transactions")
      .trigger(Trigger.ProcessingTime(1000)) // 1 segundo = 1000
      //      .trigger(Trigger.Once()) // Not supported
      .start()

    query.awaitTermination()
  }
}

object MaxOffsetsPerTrigger {
  def main(args: Array[String]): Unit = {
    val a = new MaxOffsetsPerTrigger
    a.start()
  }
}