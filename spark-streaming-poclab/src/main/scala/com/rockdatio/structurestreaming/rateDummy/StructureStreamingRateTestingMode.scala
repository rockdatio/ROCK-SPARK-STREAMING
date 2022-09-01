package com.rockdatio.structurestreaming.rateDummy

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

class StructureStreamingRateTestingMode extends Serializable {
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
      .option("rowsPerSecond", 3)
      .option("rampUpTime", 0)
      .option("numPartitions", 8)
      .load()

    rateDF.printSchema()

//    //DEBUG MODE, IN CONSOLE.
    val query = rateDF
      .writeStream
//      .outputMode("append")
//      .outputMode("complete")
      .outputMode("update")
      .format("console")
      .option("numRows", 20)
      .option("truncate", value = false)
      .start()

    query.awaitTermination()
    //    query.awaitTermination(terminationInterval)
  }
}

object StructureStreamingRateTestingMode {
  def main(args: Array[String]): Unit = {
    val a = new StructureStreamingRateTestingMode
    a.start()
  }
}