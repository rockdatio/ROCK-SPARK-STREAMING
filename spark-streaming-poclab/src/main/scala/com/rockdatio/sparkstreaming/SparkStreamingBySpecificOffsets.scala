package com.rockdatio.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang

class SparkStreamingBySpecificOffsets extends Serializable {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

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

  val inputTopic: String = "dmc-prueba2"

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "security.protocol" -> "PLAINTEXT",
    // SSL ENABLE
    // "schema.registry.trustore" -> "trustore.jks",
    // "schema.registry.keystore" -> "trustore.jks",
    "group.id" -> "spark-consumer-group-processor-applications",
    "spark.security.credentials.kafka.enabled" -> (false: java.lang.Boolean),
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val offsetRanges: Array[OffsetRange] = Array(
    // topic, partition, inclusive starting offset, exclusive ending offset
    OffsetRange("dmc-prueba2", 0, 3000, 3523),
    OffsetRange("dmc-prueba2", 1, 3000, 3228),
    OffsetRange("dmc-prueba2", 2, 3000, 3249)
  )

  def start(): Unit = {
//    val notifyDStream = KafkaUtils.createRDD[String, String](
//        sc,
//        kafkaParams,
//        offsetRanges,
//        PreferConsistent)


//    notifyDStream
//      .foreachRDD(rdd => {
//        rdd.foreachPartition {
//          recordsOfPartition => {
//            val records = recordsOfPartition.toList
//            records.foreach { message => {
//              println("# Print Each message from RDD -> PARTITION  -> MESSAGE")
//              println(message)
//            }
//            }
//          }
//        }
//      }
//      )

//    ssc.start()
//    ssc.awaitTermination()
  }
}


object SparkStreamingBySpecificOffsets {
  def main(args: Array[String]): Unit = {
    val a = new SparkStreaming
    a.start()
  }
}