package com.rockdatio.sql.rddTreatment

import com.rockdatio.sql.rddTreatment.RddExample.main
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class RddExample {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")
  def start(): Unit = {
    lazy val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.executor.instances", "5")
      .set("spark.executor.cores", "8") // slots
      .set("spark.executor.memory", "20g")
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

    val lines: RDD[String] = sc.textFile("src/resources/data/ml-100k/u.data")
    val ratings: RDD[String] = lines
      .map(x => {x.split("\t")(2)})

    val results: collection.Map[String, Long] = ratings.countByValue()

    results.foreach(resultado=> println(resultado._1, resultado._2))

    Thread.sleep(2000000)
  }
}


object RddExample {
  def main(args: Array[String]): Unit = {
    for (elem <- args) {
      println("Parametros o tablas a procesar.")
      println(elem)
    }
    val a = new RddExample
    a.start()
  }
}