package SparkStreamingDemo

//import kafka.KafkaSink
import org.apache.kafka.clients.consumer.ConsumerRecord
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

class DstremPocRock extends Serializable {
  val inputTopic: String = "json-topic"

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
  val kafkaProducerParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "security.protocol" -> "PLAINTEXT",
    "group.id" -> "producer",
    "spark.security.credentials.kafka.enabled" -> (false: java.lang.Boolean),
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

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
  val ssc = new StreamingContext(sc, Durations.seconds(1))

//  val kafkaSink: Broadcast[KafkaSink] = sc.broadcast(KafkaSink(kafkaProducerParams))

  def start(): Unit = {
    val notifyDStream: DStream[String] = KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Array(inputTopic), kafkaParams))
      .map((record: ConsumerRecord[String, String]) => record.value)
      .transform(
        (rdd: RDD[String]) => {
          if (!rdd.isEmpty()) {
            val result: DataFrame = ss.read
              .json(rdd)
            result.show()
            println("a escribir")
            result
              .write
              .mode("append")
              .option("header", "true")
              //                .partitionBy("transaction_date")
              .parquet("src/resources/data/poc/delta/delta/tabla2=hoy")

            result.toJSON.rdd
          } else rdd
        })
    notifyDStream.print()

    notifyDStream
      .foreachRDD(rdd => {
        rdd.foreachPartition {
          recordsOfPartition => {
            val records = recordsOfPartition.toList
            records.foreach { message => {
              println("hola")
//              kafkaSink.value.sendMessage("outputtopic", message)
            }
            }
          }
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}



object DstremPocRock {
  def main(args: Array[String]): Unit = {
    val a = new DstremPocRock
    a.start()
  }
}