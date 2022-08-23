package com.rockdatio.structurestreaming

//import kafka.KafkaSink

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreaming extends Serializable {
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
    //    import ss.implicits._

    val maxtrigger = 5000
    val terminationInterval = 50000L

    val kafkaDF: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:9092")
      //      .option("startingOffsets", """{"rawbadi": {"0":-2, "1":-2, "2":-2}}""")
      .option("maxOffsetsPerTrigger", maxtrigger.toLong) // CON EL RATIO DE ESCRITURA PODEMOS DEFINIR CUANTOS MENSAJES PROCESAR POR SEGUNDO, SOLO FUNCIONA EN STREAMING Y NO EN BATCH. PERO REEMPLAZARÍA EL BATCH.
      //      .option("startingOffsets", """{"rawbadi": {"0":3650000 , "1":3650000, "2":3650000}}""")//si se poneun offset superior no existente, se ejecuta en earliest  mode. Si ya se hizo una carga earliest ya no funciona el startingoffset sobre un punto.
      //      .option("startingOffsets", "earliest")
      .option("startingOffsets", """{"rawbadi": {"0":3000000, "1":3000000, "2":3000000}}""")
      .option("endingOffsets", """{"rawbadi": {"0":1000000, "1":1000000, "2":1000000}}""")
      //      .option("endingOffsets", "latest")
      //      .option("startingOffsets", "latest")
      //      .option("groupIdPrefix", "spark-kafka-groupid-consumer2")
      .option("group.id", "processor-applications2") // use it with extreme caution, can cause unexpected behavior.
      .option("failOnDataLoss", "false") // use it with extreme caution, can cause unexpected behavior.
      //      .option("startingOffsetsByTimestamp", """{"cloud-glb-shcl-marketing-campaign-debitflag":{"0": -2}}""")
      .option("subscribe", "rawbadi")
      //      .option("subscribe", "json-topic3")
      .load()

    // DEBUG MODE, IN CONSOLE.
    //        val query = kafkaDF
    //          .writeStream
    //          .outputMode("append")
    //          .format("console")
    //          .start()

    // SELECTING JUST WE NEED
    //        val query = kafkaDF
    ////          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic")
    //          .select(col("key").cast(StringType), col("value").cast(StringType))
    //          .writeStream
    //          .outputMode("append")
    //          .format("console")
    //          .start()

    // JSON FUNCTION DESERIALIZATION
    val topicSchema = StructType(
      Array(
        StructField("card_number", StringType),
        StructField("transaction_type", StringType),
        StructField("transaction_date", StringType),
        StructField("phone_number", StringType),
        StructField("amount", StringType),
        StructField("headerId", StringType)
      ))
    //  DEBUG MODE, IN CONSOLE.
    val query: StreamingQuery = kafkaDF.select(col("value").cast(StringType).alias("json"))
      .select(from_json(col("json"), topicSchema) as "data")
      .select("data.*")
      .writeStream
      .partitionBy("transaction_type")
      .format("console")
      .outputMode("append")
      .format("parquet") // supports these formats : csv, json, orc, parquet
      .option("path", "C:\\Users\\Usuario\\IdeaProjects\\spark-internals\\src\\StructureStreaminDemo\\output27")
      .option("checkpointLocation", "C:\\Users\\Usuario\\IdeaProjects\\spark-internals\\src\\StructureStreaminDemo\\checkpoint54")
      //      .trigger(Trigger.ProcessingTime(5000))
      .trigger(Trigger.ProcessingTime(1000))
      //      .trigger(Trigger.Once())
      .start()


    //    val query = kafkaDF.select(col("value").cast(StringType).alias("json"))
    //      .select(from_json(col("json"), topicSchema) as "data")
    //      .select("data.*")
    //      .select(to_avro('struct,topicSchema).as('value))
    //      .writeStream
    //
    query.awaitTermination()
    //    query.awaitTermination(terminationInterval)
  }
}

object StructureStreaming {
  def main(args: Array[String]): Unit = {
    val a = new StructureStreaming
    a.start()
  }
}