package com.rockdatio.sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

class SerilizationStreamingApp extends Serializable {
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
    val x: Broadcast[UdfPoc] = sc.broadcast(UdfPoc.apply(ss, 10))

    // USAR UN COMPANION OBJECT serializado
    // BROADCAST serializado (Optimo)

    ss
      .sql("SELECT 'Hello World!' as text").show()

    ss
      .sql("SELECT 'Hello World!' as text")
      .withColumn("rotated_text", UdfPoc.apply(ss, 10).rotateStringUdf(col("text")))
      .show()
  }
}

object SerilizationStreamingApp {
  def main(args: Array[String]): Unit = {
    val a = new SerilizationStreamingApp
    a.start()
  }
}

