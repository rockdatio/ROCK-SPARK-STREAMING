package com.rockdatio.structurestreaming.foreachWriter

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingSqlHdfsMultipleWriter extends Serializable {
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
    //    import ss.implicits._
    val inputTopic = "rawbadi2"

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("startingOffsets", "earliest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
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
    // DEBUG MODE, IN CONSOLE.
    val dfFinal: DataFrame = kafkaDF
      .select(col("value").cast(StringType).alias("json"))
      .select(from_json(col("json"), topicSchema) as "data")
      .select("data.*")

//    val query = dfFinal
//      .writeStream.
//      .outputMode("append")
//      .foreachBatch{ (batchDF: DataFrame, batchId: Long) => {
//      batchDF.persist()
//      batchDF
//        .partitionBy("transaction_type")
//        .format("parquet") // supports these formats : csv, json, orc, parquet
//        .option("path", s"src/resources/datalkeSS/${inputTopic}/transactions")
//    }
//      .trigger(Trigger.ProcessingTime(1000)) // 1 segundo = 1000
//      //      .trigger(Trigger.Once())
//      .start()
//
//    query.awaitTermination()
  }
}

