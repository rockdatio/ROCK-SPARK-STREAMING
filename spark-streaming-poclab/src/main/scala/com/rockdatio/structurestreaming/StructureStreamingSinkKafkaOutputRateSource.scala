package com.rockdatio.structurestreaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingSinkKafkaOutputRateSource extends Serializable {
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

    val rateDF: DataFrame = ss.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .option("rampUpTime", 10)
      .option("numPartitions", 8) // Use it to measure kafka partitions
      .load()

    rateDF.printSchema()
    val clenaDF = rateDF
      .select(
        col("value").cast(StringType).alias("value"), // we must send at least "value" column
        col("timestamp")
      )
      .select("value", "timestamp")

    clenaDF.printSchema()

    //DEBUG MODE, IN CONSOLE.
    val query = clenaDF
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 20)
      .option("truncate", value = false)
      .option("checkpointLocation", s"src/resources/datalake/console/transactions")
      .start()

    val outputTopic = "output-topic"

    //    val query = clenaDF.writeStream
    //      .queryName("kafkaWriter")
    //      .outputMode("append")
    //      .format("kafka") // determines that the kafka sink is used
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("topic", outputTopic)
    //      .option("checkpointLocation", s"src/resources/datalake/structure-streaming/${outputTopic}/transactions")
    //      .option("failOnDataLoss", "false") // use this option when testing
    //      .start()


    query.awaitTermination()
    //    query.awaitTermination(terminationInterval)
  }
}

object StructureStreamingSinkKafkaOutputRateSource {
  def main(args: Array[String]): Unit = {
    val a = new StructureStreamingSinkKafkaOutputRateSource
    a.start()
  }
}