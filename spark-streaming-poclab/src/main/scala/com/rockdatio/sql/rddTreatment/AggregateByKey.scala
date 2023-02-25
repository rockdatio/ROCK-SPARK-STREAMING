package com.rockdatio.sql.rddTreatment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class AggregateByKey {
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

    val carsRDD: RDD[(String, String, Int)] = sc.parallelize(Array(
      ("Hyundai", "Verna", 2000), ("Hyundai", "Creta", 3000), ("Hyundai", "i20", 9100), ("Hyundai", "i10", 8200),
      ("Hyundai", "Santro", 6900), ("Maruthi", "Swift", 62000), ("Maruthi", "Baleno", 9007), ("Maruthi", "Breeza", 80000),
      ("Maruthi", "Ertiga", 70018), ("Tata", "Nexon", 71903), ("Tata", "Safari", 16008), ("Tata", "Tiago", 80347)), 3)

    println("hola")
    println(carsRDD.getNumPartitions)

    carsRDD.collect()

    //Defining Seqencial Operation and Combiner Operations
    //Sequence operation : Finding Maximum Sales from a single partition
    def seqOp = (accumulator: Int, element: (String, Int)) =>
      if (accumulator > element._2) accumulator else element._2

    //Combiner Operation : Finding Maximum Sales out Partition-Wise Accumulators
    def combOp = (accumulator1: Int, accumulator2: Int) =>
      if (accumulator1 > accumulator2) accumulator1 else accumulator2

    //Zero Value: Zero value in our case will be 0 as we are finding Maximum Sales
    val zeroVal = 0
    val aggrRDD = carsRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroVal)(seqOp, combOp)
    aggrRDD.collect foreach println

    Thread.sleep(200000)
  }
}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val a = new AggregateByKey
    a.start()
  }
}