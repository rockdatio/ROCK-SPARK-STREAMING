package com.rockdatio.structurestreaming

//import kafka.KafkaSink

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingConsoleOutput extends Serializable {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

  def start(): Unit = {
    lazy val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("streaming Test")

    @transient lazy val ss: SparkSession = SparkSession
      .builder()
      .appName("streaming Test")
      .config(conf)
      .getOrCreate()
    val sc: SparkContext = ss.sparkContext

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      .option("startingOffsets", "earliest")
      //      .option("endingOffsets", "latest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
      .option("subscribe", "rawbadi")
      .load()

    //DEBUG MODE, IN CONSOLE.
    val query = kafkaDF
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
    //    query.awaitTermination(terminationInterval)
  }
}

