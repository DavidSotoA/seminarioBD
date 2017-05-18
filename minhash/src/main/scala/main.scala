package com.minhash

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FraudDetection {

  def fraudDetection(
    instances: DataFrame,
    spark: SparkSession,
    numOfHashFunctions: Int,
    primesUrl: String,
    numOfBands: Int): DataFrame = {
      // poner shingling
      val minhashDF = Minhash.minhash(instances, numOfHashFunctions, spark, primesUrl)
      minhashDF.persist(MEMORY_ONLY_SER)
      Lsh.lsh(numOfBands, minhashDF, spark)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder()
     .appName(Constants.APP_NAME)
     .enableHiveSupport()
     .getOrCreate()
  }
}
