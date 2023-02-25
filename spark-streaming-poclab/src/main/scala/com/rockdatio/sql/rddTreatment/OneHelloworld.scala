package com.rockdatio.sql.rddTreatment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class OneHelloworld {
  System.setProperty("hadoop.home.dir", "c:\\winutil\\")
// EJEMPLO 1 DE COMO CONFIGURAR MI CLUSTER DE SPARK
  //  TOTAL CLUSTER :
//    5 EXECUTORS
//    8 CORES
//      EXECUTOR MEMORY 20GB
//
//      CAPACIDAD TOTAL .
//  MEMORIA TOTAL = 5 * 20 = 100 GB RAM COMO TOTALIDAD
//  CAPACIDAD MAXIMA DE PROCESAMIENTO EN PARALELO = 5 * 8 = 40 CORES/slots /hilos de procesamiento

// EJM 2:
// CUANTA ES LA CAPACIDAD QUE DEBO USAR PARA GATILLAR UNA APLICACIÓN SPARK
// 1. SE TIENE COMO FUENTE O COMO DATA SOURCE 1 TB DATOS /  2 000 000
// 2. MAPEO DE COLUMNAS, AGREGAR NUEVAS COLUMNAS, LIMPIAR FILAS
// 5  EXECUTORS
//  5  EXECUTOR MEMORY 256GB
//  CAPACIDAD MAXIMA DE PROCESAMIENTO EN PARALELO = 5 * 8 = 40 CORES/slots /hilos de procesamiento (40 PARTICIONES)
//  8 CORES
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

    val lines: RDD[String] = sc.textFile(s"src/resources/data/ml-100k/u.data", 180)
    println(lines.getNumPartitions)

    val ratings: RDD[String] = lines.map((row: String) => {
      //      val result: Array[String] = row.split("\t")
      //      result.foreach(println(_))
      row.split("\t")(2)
    })
  //transformaccion 1 .. 100

    // LAZY EVALUATION
    ratings.filter((rating: String) => rating == "2" ).collect().foreach(println(_))


    Thread.sleep(2000000)
  }
}

object OneHelloworld {
  def main(args: Array[String]): Unit = {
    for (elem <- args) {
      println("Parametros o tablas a procesar.")
      println(elem)
    }
    val a = new OneHelloworld
    a.start()
  }
}