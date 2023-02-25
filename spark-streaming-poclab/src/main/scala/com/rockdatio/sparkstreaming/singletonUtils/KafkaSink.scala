package com.rockdatio.sparkstreaming.singletonUtils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import java.util.concurrent.Future

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer: KafkaProducer[String, String] = createProducer()

  def sendMessage(topic: String, value: String): Future[RecordMetadata] = {
    producer.send(new ProducerRecord(topic, value))
  }
}

object KafkaSink {

  def apply(): KafkaSink = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val startAndCloseProducerClient: () => KafkaProducer[String, String] = () => {
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(startAndCloseProducerClient)
  }
}
