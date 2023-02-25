package com.rockdatio.structurestreaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col}

class OutputModeKafkaSourceConsoleOutput extends Serializable {
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
      .option("startingOffsets", "earliest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
      .option("subscribe", inputTopic)
      .load()

    //DEBUG MODE, IN CONSOLE.
    val query = kafkaDF
      .select(col("value").cast(StringType)) // Deserialization
      .writeStream
      .outputMode("append")
      // outputMode update is only useful with Aggregations
      // outputMode Complete is only useful with Aggregations
      .format("console")
      .option("numRows", 20)
      .option("truncate", value = false)
      .trigger(Trigger.ProcessingTime(10000)) // 1 segundo = 1000
      .start()

    query.awaitTermination()
    //    query.awaitTermination(terminationInterval)
  }
}

object OutputModeKafkaSourceConsoleOutput {
  def main(args: Array[String]): Unit = {
    val a = new OutputModeKafkaSourceConsoleOutput
    a.start()
  }
}