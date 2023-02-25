package com.rockdatio.sparkstreaming

import com.rockdatio.sparkstreaming.singletonUtils.KafkaSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

class SparkStreamingSinkProducer extends Serializable {
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

  val inputTopic: String = "dmc-realtime"
  val outputTopic: String = "dmc-realtime-sink"

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
    "security.protocol" -> "PLAINTEXT",
    "group.id" -> "processor-applications-0.10.2",
    "spark.security.credentials.kafka.enabled" -> (false: java.lang.Boolean),
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def start(): Unit = {
//    val props = new Properties()
//    props.put("bootstrap.servers", "localhost:9092")
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    val kafkaSink: Broadcast[KafkaSink] = sc.broadcast(KafkaSink())

    val notifyDStream: DStream[String] = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Array(inputTopic), kafkaParams))
      .map(
        (record: ConsumerRecord[String, String]) => record.value)
    notifyDStream.print()

    notifyDStream
      .foreachRDD(rdd => {
        rdd.foreachPartition {
          recordsOfPartition => {
            val records = recordsOfPartition.toList

            records.foreach { message => {
              println("# Print Each message from RDD -> PARTITION  -> MESSAGE")
              println(message)
              kafkaSink.value.sendMessage(outputTopic, message)
//              producer.send(new ProducerRecord(outputTopic, message))
            }
            }
          }
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

object SparkStreamingSinkProducer {
  def main(args: Array[String]): Unit = {
    val a = new SparkStreamingSinkProducer
    a.start()
  }
}

