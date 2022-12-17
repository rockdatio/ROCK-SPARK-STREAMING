package com.rockdatio.sql

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class UdfPoc(nRotations: Integer, logger: () => Logger) extends Serializable {
  def rotateStringUdf: UserDefinedFunction = {
    logger.apply().info("Initializing StringRotatorJob")

    udf { str: String =>
      logger.apply().info("jajajaajaj")
      str.substring(nRotations) + str.substring(0, nRotations)
    }
  }
}

object UdfPoc {
  def apply(spark: SparkSession, nRotationsConst: Integer): UdfPoc = {
    val logger: () => Logger = () => {
      LogManager.getLogger(getClass)
    }

//    val logger = LogManager.getLogger(getClass)

    new UdfPoc(nRotationsConst, logger)
  }
}