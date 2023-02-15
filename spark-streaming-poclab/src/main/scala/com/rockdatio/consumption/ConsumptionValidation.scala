package com.rockdatio.consumption

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

class ConsumptionValidation {
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
    val inputTopic = "dmc-realtime"

    val df = ss
      .read
      .format("parquet")
      .load(s"src/resources/datalakeSS/${inputTopic}/transactions")
      .repartition(180)
      .cache()

//    println(df.repartition(180).rdd.getNumPartitions)
//    df.show()
    println(df.count())

//    println(df.rdd.getNumPartitions)
    Thread.sleep(20000)
  }
}

object ConsumptionValidation {
  def main(args: Array[String]): Unit = {
    val a = new ConsumptionValidation
    a.start()
  }
}