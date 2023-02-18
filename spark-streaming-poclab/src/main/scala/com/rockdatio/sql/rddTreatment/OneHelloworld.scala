package com.rockdatio.sql.rddTreatment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class OneHelloworld {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

  def start(): Unit = {
    lazy val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
//      .set("spark.executor.memory", "8g")
      .set("spark.executor.cores", "10") // slots
      .set("spark.executor.instances", "1")
      .set("spark.cores.max", "8")
      .set("spark.deploy.defaultCores", "8")
      .set("spark.driver.memory", "8g")
      .setAppName("streaming Test")

    @transient lazy val ss: SparkSession = SparkSession
      .builder()
      .appName("streaming Test")
      .config(conf)
      .getOrCreate()
    val sc: SparkContext = ss.sparkContext


    Thread.sleep(20000)
  }
}

object OneHelloworld {
  def main(args: Array[String]): Unit = {
    val a = new OneHelloworld
    a.start()
  }
}