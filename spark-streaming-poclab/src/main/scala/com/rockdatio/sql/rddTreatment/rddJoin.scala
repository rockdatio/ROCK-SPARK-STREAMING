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

    val rdd1: RDD[Products] = sc.parallelize(Seq(
      Products("product1", "category1"),
      Products("product2", "category2"),
      Products("product3", "category3"),
      Products("product4", "category4")
    ))

    val rdd2: RDD[Customer] = sc.parallelize(Seq(
      Customer("customer1", "product1", 5),
      Customer("customer1", "product2", 6),
      Customer("customer2", "product3", 2),
      Customer("customer2", "product4", 9)
    ))

    val transformedRDD1: RDD[(String, Products)] = rdd1
      .map(
        productos =>
          (productos.productId, productos) //OBTENGO UN TUPLA LLAVE VALOR (PRODUCTID, PRODUCTOS)
      )

    val transformedRDD2: RDD[(String, Customer)] = rdd2
      .map(
        customer =>
          (customer.productId, customer)
      )


    val joinedFinal: RDD[(String, (Products, Customer))] = transformedRDD1.join(transformedRDD2)

    val cleanedJoined: RDD[(String, String, String, Int)] = joinedFinal
      .map(x =>
        (x._1, x._2._1.category, x._2._2.customerId,x._2._2.quantity)
      )

    cleanedJoined.collect().foreach(println(_))

    Thread.sleep(2000000)
  }
}

object rddJoin {
  def main(args: Array[String]): Unit = {
    val a = new rddJoin
    a.start()
  }
}