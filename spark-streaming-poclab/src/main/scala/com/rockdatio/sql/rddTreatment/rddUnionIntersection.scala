package com.rockdatio.sql.rddTreatment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class rddUnionIntersection {
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

    val rddProduct: RDD[(String, String)] = sc.parallelize(Seq(
      ("product1", "category1"),
      ("product2", "category2"),
      ("product3", "category3"),
      ("product4", "category4"),
    ))

    val rddCustomer: RDD[(String, String)] = sc.parallelize(Seq(
      ("customer1", "product1"),
      ("customer1", "product2"),
      ("customer2", "product3"),
      ("customer2", "product4"),
      ("product4", "category4")
    ))

    // PARA USAR EL UNION LOS RDDs deben ser iguales y del mismo tipo de dato
    val resultado: Array[(String, String)] = rddProduct.union(rddCustomer).collect()
//    resultado.foreach(println(_))

    val resultado2 = rddProduct.intersection(rddCustomer).collect()
    resultado2.foreach(println(_))


    Thread.sleep(20000)
  }
}

object rddUnionIntersection {
  def main(args: Array[String]): Unit = {
    val a = new rddUnionIntersection
    a.start()
  }
}