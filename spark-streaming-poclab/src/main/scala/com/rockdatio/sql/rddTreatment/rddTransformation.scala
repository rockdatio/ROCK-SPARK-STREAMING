package com.rockdatio.sql.rddTreatment

import com.rockdatio.sql.rddTreatment.rddTransformation.main
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.min

class rddTransformation {
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

    val lines: RDD[String] = sc.textFile(s"src/resources/data/ml-100k/u.data")
    println(lines.getNumPartitions)

    val ratings: RDD[String] = lines.map((row: String) => {
      //      val result: Array[String] = row.split("\t")
      //      result.foreach(println(_))
      row.split("\t")(2)
    })

    println("Filter")
    ratings.filter((rating: String) => rating == "2" ).collect().foreach(println(_))

    println("DISTINCT")
    ratings.distinct().collect().foreach(println(_))



    println("SAMPLE")
//    val sample: RDD[String] = lines.sample(withReplacement = false, 0.05, 1)


    val z: Array[String] = lines.take(10)
    println("TAKE")
    z.foreach(println(_))
//    sample.collect().foreach(println(_))

    val results: collection.Map[String, Long] = ratings.countByValue()
    val sortedValues: Seq[(String, Long)] = results.toSeq.sortBy(_._2)
    println("sorted values")
    sortedValues.foreach(println(_))


    val result2: RDD[(String, Long)] = lines.map(row => row.split("\t")(2))
      .map((row: String) => (row, 1L)).reduceByKey(_ + _)
    val result3: RDD[(String, Iterable[Long])] = lines.map(row => row.split("\t")(2)).map((row: String) => (row, 1L)).groupByKey()
    val sumTotal: Array[(String, Long)] = result3.mapValues((f: Iterable[Long]) => f.sum).collect()
    println("groupbykey")
    sumTotal.foreach(println(_))
    results.foreach(println(_))
    result2.foreachPartition((f: Iterator[(String, Long)]) => f.foreach(println(_)))

    Thread.sleep(20000)
  }
}

object rddTransformation {
  def main(args: Array[String]): Unit = {
    val a = new rddTransformation
    a.start()
  }
}




