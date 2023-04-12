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

    val lines: RDD[String] = sc.textFile(s"src/resources/data/ml-100k/u.data", 180)

    //    println("SAMPLE")
    //    val sample: RDD[String] = lines.sample(withReplacement = false, 0.5, 6)
    //
    //    val takeResult: Array[String] = lines.take(10)
    //    println("TAKE")
    //    takeResult.foreach(println(_))
    //    sample.collect().foreach(println(_))


    //    println("DISTINCT")
    //    ratings.distinct().collect().foreach(println(_))
    //    (key, value)
    //    (key, value1, value2, value3)
    //    (key, value1, value2)
    //    (value2)
    val ratings: RDD[String] = lines.map((row: String) => {
      //      val result: Array[String] = row.split("\t")
      //      result.foreach(println(_))
      row.split("\t")(2)
    })

    val count: collection.Map[String, Long] = ratings.countByValue()
    count.toSeq.sortBy(_._1).foreach(println(_))

    //    println("Filter")
    //    ratings.filter(rating => rating == "2" ).collect().foreach(println(_))
    //

    //
    //
    (1,6110)
    (2,11370)
    (3,27145)
    (4,34174)
    (5,21201)
    //    val results: collection.Map[String, Long] = ratings.countByValue()
    //    val sortedValues: Seq[(String, Long)] = results.toSeq.sortBy(_._2)
    //    println("sorted values")
    //    sortedValues.foreach(println(_))
    //
    //
    val result2: RDD[(String, Long)] = lines
      .map(row => row.split("\t")(2))
      .map((row: String) => (row, 1L))
      .reduceByKey(_ + _)

//    result2.foreachPartition((f: Iterator[(String, Long)]) => f.foreach(println(_)))
    result2.collect().foreach(println(_))


  // CON ESO FINALIZAMOS EL CURSO DE AP SPARK
    // GRACIAS POR SU ATENCION
    // HEMOS APRENDIDO LAS BASES DE APACHE SPARK Y CON ESTO SIENTANSE EN LIBERTAD
    // DE ENFRENTARSE A GRANDES PROYECTOS
    // NOS VEMOS EN UN SIGUIENTE CURSO

    val result3 = lines.map(row => row.split("\t")(2)).map((row: String) => (row, 1L)).groupByKey()
    val sumTotal = result3.mapValues((f: Iterable[Long]) => f.sum).collect()
    println("groupbykey")
    sumTotal.foreach(println(_))


    //
    Thread.sleep(2000000)
  }
}

object rddTransformation {
  def main(args: Array[String]): Unit = {
    val a = new rddTransformation
    a.start()
  }
}




