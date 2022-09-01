package com.rockdatio.consumption

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

class ConsumptionValidation {
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
    val inputTopic = "rawbadi2"

    // card_number#0,transaction_date#1,phone_number#2,amount#3,headerId#4,transaction_type#
    val df = ss
      .read
      .format("parquet")
      .load(s"src/resources/datalkeSS/${inputTopic}/transactions")

    val df1 = df.select(col("card_number"), col("phone_number"), col("transaction_type"))
    val df2 = df1.select(col("card_number"), col("transaction_type"))
    val df3 = df2.select(col("card_number"))

    println(df.rdd.getNumPartitions)
    println(df3.queryExecution)

    df3.count()
    Thread.sleep(20000)
//    println(
//      ss
//      .read
//      .format("parquet")
//      .load(s"src/resources/datalkeSS/${inputTopic}/transactions").count()
//    )
  }
}

object ConsumptionValidation {
  def main(args: Array[String]): Unit = {
    val a = new ConsumptionValidation
    a.start()
  }
}