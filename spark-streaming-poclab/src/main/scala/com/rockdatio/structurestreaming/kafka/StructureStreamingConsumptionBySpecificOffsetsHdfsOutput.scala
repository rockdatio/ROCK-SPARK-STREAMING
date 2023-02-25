package com.rockdatio.structurestreaming.kafka

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingConsumptionBySpecificOffsetsHdfsOutput extends Serializable {
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

  def start(): Unit = {
    val inputTopic = "dmc-realtime"

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("startingOffsets", """{"dmc-realtime": {"0":12000, "1":12000, "2":12000}}""")
      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
      //      .option("startingOffsetsByTimestamp", """{"cloud-glb-shcl-marketing-campaign-debitflag":{"0": -2}}""")
      .option("subscribe", inputTopic)
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
      .select(
        col("value").cast(StringType).alias("json")
      )
      .select(from_json(col("json"), topicSchema) as "data")
      .select("data.*")
      .writeStream
      .partitionBy("transaction_type")
      .format("console")
      .outputMode("append")
      .format("parquet") // supports these formats : csv, json, orc, parquet
      .option("path", s"src/resources/datalakeSS/${inputTopic}/transactions")
      .option("checkpointLocation", s"src/resources/datalakeSS/${inputTopic}/checkpoint/transactions")
      //      .trigger(Trigger.ProcessingTime(5000))
      .trigger(Trigger.ProcessingTime(1000))
      //      .trigger(Trigger.Once())
      .start()

    query.awaitTermination()
  }
}

object StructureStreamingConsumptionBySpecificOffsetsHdfsOutput {
  def main(args: Array[String]): Unit = {
    val a = new StructureStreamingConsumptionBySpecificOffsetsHdfsOutput
    a.start()
  }
}