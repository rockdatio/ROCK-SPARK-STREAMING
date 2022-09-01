package com.rockdatio.structurestreaming.windowoperation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class WindowAgregationsTumblingSql extends Serializable {
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
      .option("startingOffsets", "latest")
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
    val cleanDf: DataFrame = kafkaDF
      .select(col("value").cast(StringType).alias("json"))
      .select(from_json(col("json"), topicSchema) as "data")
      .select("data.*")

    val windowedDF = cleanDf.groupBy(
      window(
        col("transaction_date"),
        "15 seconds"),
      cleanDf("transaction_type")
    ).count()
    //      .agg(max(bround(cleanDf("amount"), 2)).as("RevenueUSD"))

    //    //DEBUG MODE, IN CONSOLE.
    val query = windowedDF
      .writeStream
      //      .outputMode("append")
            .outputMode("complete")
//      .outputMode("update")
      .format("console")
      .option("numRows", 20)
      .option("truncate", value = false)
      .start()

//    val query = windowedDF
//      .writeStream
//      .partitionBy("transaction_type")
////      .format("console")
////      .outputMode("append") // Append output mode not supported when there are streaming aggregations without watermark
////      .outputMode("update") //  Data source parquet does not support Update output mode;
//      .outputMode("complete") // Data source parquet does not support Complete output mode;
////      .format("parquet") // supports these formats : csv, json, orc, parquet
//      .option("path", s"src/resources/datalkeSS/${inputTopic}/transactions")
//      .option("checkpointLocation", s"src/resources/datalkeSS/${inputTopic}/checkpoint/transactions")
//      .trigger(Trigger.ProcessingTime("15 seconds")) // 1 segundo = 1000
//      //      .trigger(Trigger.Once()) // not supported
//      .start()

    query.awaitTermination()
  }
}

object WindowAgregationsTumblingSql {
  def main(args: Array[String]): Unit = {
    val a = new WindowAgregationsTumblingSql
    a.start()
  }
}

