package com.minhash

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Minhash {
  def main(args: Array[String]): Unit = {
    val spark = initSparkSession
    val numOfHashFunctions = args(0).toInt
    val direc = args(1)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder()
     .appName(Constants.APP_NAME)
     .enableHiveSupport()
     .getOrCreate()
  }

}
