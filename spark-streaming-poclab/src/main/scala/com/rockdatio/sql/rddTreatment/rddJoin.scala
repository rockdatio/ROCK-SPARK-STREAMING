package com.rockdatio.sql.rddTreatment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class Products(productId:String, category:String)
case class Customer(customerId:String, productId:String, quantity:Int)

class rddJoin {
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

    val rdd1 = sc.parallelize(Seq(
      Products("product1", "category1"),
      Products("product2", "category2"),
      Products("product3", "category3"),
      Products("product4", "category4")
    ))
    val rdd2 = sc.parallelize(Seq(
      Customer("customer1", "product1", 5),
      Customer("customer1", "product2", 6),
      Customer("customer2", "product3", 2),
      Customer("customer2", "product4", 9)
    ))

    val transformedRDD1: RDD[(String, Products)] = rdd1.map(prod => (prod.productId, prod))
    val transformedRDD2: RDD[(String, Customer)] = rdd2.map(customer => (customer.productId, customer))



    val joined: RDD[(String, (Products, Customer))] = transformedRDD1
      .join(transformedRDD2)

    val cleanedJoined: RDD[(String, String, Int)] = joined
      .map(x => (x._2._2.customerId, x._2._1.category, x._2._2.quantity))

    cleanedJoined.saveAsTextFile(s"src/resources/output/data_join/")

    Thread.sleep(20000)
  }
}

object rddJoin {
  def main(args: Array[String]): Unit = {
    val a = new rddJoin
    a.start()
  }
}