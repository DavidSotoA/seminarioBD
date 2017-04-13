package com.minhash

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val spark = initSparkSession
    val numOfHashFunctions = args(0).toInt
    val primesUrl = args(1)
    val readUrl = args(2)
    val saveUrl = args(3)
    val instances = spark.read.load(readUrl)
    val minhashDF = Minhash.minhash(instances, numOfHashFunctions, spark, primesUrl)
    minhashDF.write.mode(SaveMode.Overwrite).format("parquet").save(saveUrl)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder()
     .appName(Constants.APP_NAME)
     .enableHiveSupport()
     .getOrCreate()
  }
}
