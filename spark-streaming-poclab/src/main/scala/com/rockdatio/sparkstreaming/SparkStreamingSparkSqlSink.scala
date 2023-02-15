package com.rockdatio.sparkstreaming

import com.rockdatio.sparkstreaming.singletonUtils.SqlAzureSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class SparkStreamingSparkSqlSink extends Serializable {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

  val inputTopic: String = "dmc-realtime"

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "security.protocol" -> "PLAINTEXT",
    "group.id" -> "SparkStreamingSparkSqlSink",
    "spark.security.credentials.kafka.enabled" -> (false: java.lang.Boolean),
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("streaming Test")

  @transient lazy val ss: SparkSession = SparkSession
    .builder()
    .appName("streaming Test")
    .config(conf)
    .getOrCreate()

  @transient val sc: SparkContext = ss.sparkContext
  @transient val ssc = new StreamingContext(sc, Durations.seconds(1)) // @transient  an denote a field that shall not be serialized

  def start(): Unit = {
    val notifyDStream: DStream[String] = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Array(inputTopic), kafkaParams))
      .map(
        (record: ConsumerRecord[String, String]) => record.value)
      .transform(
        (rdd: RDD[String]) => {
          if (!rdd.isEmpty()) {
            val result: DataFrame = ss.read
              .json(rdd)
            result.show()
            result.printSchema()
            println("# Escribiendo to HDFS")

            result.select(col("transaction_type"), col("transaction_type")).filter("amount <= 5000").show()

            result
              .write
              .mode("append")
              .option("header", "true")
              .partitionBy("transaction_type")
              .parquet(s"src/resources/datalke/${inputTopic}/transactions")

            result.toJSON.rdd
          } else rdd
        })
    notifyDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
object SparkStreamingSparkSqlSink {
  def main(args: Array[String]): Unit = {
    val a = new SparkStreamingSparkSqlSink
    a.start()
  }
}


