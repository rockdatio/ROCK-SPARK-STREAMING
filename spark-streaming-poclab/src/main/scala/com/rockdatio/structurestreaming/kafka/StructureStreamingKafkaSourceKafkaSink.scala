package com.rockdatio.structurestreaming.kafka

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingKafkaSourceKafkaSink extends Serializable {
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
    val inputTopic = "salesforce2"
    val outputTopic = "marketing"

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("startingOffsets", "earliest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
      .option("subscribe", inputTopic)
      .load()


    val cleanDF: DataFrame = kafkaDF
      .select(
        col("value").cast(StringType).alias("value") // we must send at least "value" column
//        col("timestamp")
      )
      .select("value")

    //DEBUG MODE, IN CONSOLE.
    //    val query = kafkaDF
    //      .writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

    val query = cleanDF.writeStream
      .queryName("kafkaWriter")
      .outputMode("append")
      .format("kafka") // determines that the kafka sink is used
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", outputTopic)
      .option("checkpointLocation", s"src/resources/datalake/structure-streaming/${outputTopic}/transactions")
      .option("failOnDataLoss", "false") // use this option when testing
      // Default trigger (runs micro-batch as soon as it can)
      .start()

    query.awaitTermination()
  }
}

object StructureStreamingKafkaSourceKafkaSink {
  def main(args: Array[String]): Unit = {
    val a = new StructureStreamingKafkaSourceKafkaSink
    a.start()
  }
}