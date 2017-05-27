package com.minhash

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FraudDetection {

  def fraudDetection(
    spark: SparkSession,
    numOfHashFunctions: Int,
    primesUrl: String,
    numOfBands: Int,
    booksPath: String,
    shinglinUrl: String,
    documentosUrl: String,
    stopWordsUrl: String,
    shinglinSize: Int): DataFrame = {
      Shinglin.shinglin(booksPath,shinglinUrl,documentosUrl,stopWordsUrl,shinglinSize,spark)
      val instances = spark.sqlContext.read.parquet(shinglinUrl).toDF("j", "i")
      val minhashDF = Minhash.minhash(instances, numOfHashFunctions, spark, primesUrl)
      minhashDF.write.mode(SaveMode.Overwrite).format("parquet").save("minhash")
      val df = spark.sqlContext.read.parquet("minhash")
      Lsh.lsh(numOfBands, df, spark)
    }

    def initSparkSession(): SparkSession = {
      SparkSession.builder()
      .appName(Constants.APP_NAME)
      .enableHiveSupport()
      .getOrCreate()
    }
  }
