package com.minhash

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

case class HashFunctionDAO(a: Int, b: Int, m: Int)

object Minhash{

  // por ahora
  def providedHashFunctions(): List[HashFunctionDAO] = {
    val h1 = HashFunctionDAO(2, 1, 7)
    val h2 = HashFunctionDAO(3, 2, 7)
    val h3 = HashFunctionDAO(4, 3, 7)
    List(h1, h2, h3)
  }

  def udfsHash(hashFunctions: List[HashFunctionDAO]): List[org.apache.spark.sql.expressions.UserDefinedFunction] ={
    var udfs = List[org.apache.spark.sql.expressions.UserDefinedFunction]()
    for (i <- 0 to (hashFunctions.size - 1) ) {
      val h = hashFunctions(i)
      val partiallyevaluateFunction = evaluateFunction( _ : Int, h)
      val transformUDF = udf(partiallyevaluateFunction)
      udfs = udfs :+ transformUDF
    }
    udfs
  }

  def createQueryString(numOfHashFunctions: Int): String = {
    var queryStr = "select j as doc"
    for (i <- 1 to (numOfHashFunctions + 1)) {
      if (i == 1) {
        queryStr = queryStr + ", min(i) as f" + i
      } else {
        queryStr = queryStr + ", min(h" + (i - 2) +") as f" + i
      }
    }
    queryStr + " from df group by j"
  }

  def minhash(instances: DataFrame, numOfHashFunctions: Int, spark: SparkSession): DataFrame = {
    val instancesByRowIndex = instances.select("i").distinct
    val udfs = udfsHash(providedHashFunctions)
    var hashDF = instancesByRowIndex
    val HASH = "h"
    for (i <- 0 to (udfs.size -1)) {
      val transformUDF =  udfs(i)
      hashDF = hashDF.withColumn((HASH + i), transformUDF(hashDF("i")))
    }
    val joinDF = instances.join(hashDF, "i")
    joinDF.createOrReplaceTempView("df")
    spark.sql(createQueryString(numOfHashFunctions))
  }

  def evaluateFunction(x: Int, h: HashFunctionDAO): Int = {
    return (h.a * x + h.b) % h.m
  }

}
