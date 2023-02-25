package com.rockdatio.structurestreaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class KafkaSourceDataframeOutput extends Serializable {
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
    val inputTopic = "dmc-prueba2"

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("startingOffsets", "earliest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false")
      .option("subscribe", inputTopic)
      .load()

    val cleanDF: DataFrame = kafkaDF
      .select(
        col("value").cast(StringType).alias("data") // we must send at least "value" column
        //        col("timestamp")
      )
      .select("data")

    //    DEBUG MODE, IN CONSOLE.
    val query = cleanDF
      .writeStream
      .outputMode("append")
      .option("truncate", value = false)
      .format("console")
      .start()


    query.awaitTermination()
  }
}

object KafkaSourceDataframeOutput {
  def main(args: Array[String]): Unit = {
    val a = new KafkaSourceDataframeOutput
    a.start()
  }
}
