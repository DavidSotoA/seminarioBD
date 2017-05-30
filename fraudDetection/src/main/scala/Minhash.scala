package com.minhash

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

case class HashFunctionDAO(a: Int, b: Int, m: Int)

object Minhash{

  def minhash(
    instances: DataFrame,
    numOfHashFunctions: Int,
    spark: SparkSession,
    primeUrl: String): DataFrame = {
      val instancesByRowIndex = instances.select(Constants.INDEX_ROW).distinct
      val maxValue = instancesByRowIndex.count
      val udfs = udfsHash(createHashFunctions(maxValue, numOfHashFunctions, primeUrl))
      var hashDF = instancesByRowIndex
      val HASH = "h"
      for (i <- 0 to (udfs.size -1)) {
        val transformUDF =  udfs(i)
        hashDF = hashDF.withColumn((HASH + i), transformUDF(hashDF(Constants.INDEX_ROW)))
      }
      val joinDF = instances.join(hashDF, Constants.INDEX_ROW)
      joinDF.createOrReplaceTempView("df")
      spark.sql(createQueryString(numOfHashFunctions))
  }

  def evaluateFunction(x: String,  h: HashFunctionDAO) : String = {
      return ((h.a * BigInt(x) + h.b)%BigInt("170141183460469231731687303715884105727")).toString
  }

  def udfsHash(hashFunctions: List[HashFunctionDAO]): List[org.apache.spark.sql.expressions.UserDefinedFunction] ={
    var udfs = List[org.apache.spark.sql.expressions.UserDefinedFunction]()
    for (i <- 0 to (hashFunctions.size - 1) ) {
      val h = hashFunctions(i)
      val partiallyevaluateFunction = evaluateFunction( _ : String, h)
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

  def findPrimeNumber(maxValue: Long, primeList: List[String]): Int = {
    val valueList = primeList.find(_.toInt>maxValue)
    return valueList.toList(0).toInt
  }

  def createHashFunctions(
  maxValue: Long,
  numOfHashFunctions: Int,
  dirPrimes: String): List[HashFunctionDAO] = {
    //require((maxValue <= Constants.LAST_PRIME),
    //"El número primo requerido(mayor a" + maxValue + ") no esta disponible")
    //val primList = scala.io.Source.fromFile(dirPrimes).getLines.toList
    //val p = findPrimeNumber(maxValue, primList)
    val p = 1318699
    val r = scala.util.Random
    var hashFunctions =  List[HashFunctionDAO]()
    for (i <- 1 to (numOfHashFunctions)) {
      val a = r.nextInt(numOfHashFunctions * 2)
      val b = r.nextInt(numOfHashFunctions * 2)
      val hashFunction = new HashFunctionDAO(a, b , p)
      hashFunctions = hashFunctions :+ hashFunction
    }
    hashFunctions
  }

  def createDisperseMatrix(rows: Int, cols: Int, disperCoef: Double): List[(Int, Int)] = {
    val r = scala.util.Random
    var x = List[(Int, Int)]()
    for (i <- 1 to rows) {
      for (j <- 1 to cols) {
        val rnd =  r.nextFloat
        if (rnd < disperCoef){
          x =  x :+ (i, j)
        }
      }
    }
    x
  }

  // por ahora
  def providedHashFunctions(): List[HashFunctionDAO] = {
    val h1 = HashFunctionDAO(2, 1, 7)
    val h2 = HashFunctionDAO(3, 2, 7)
    val h3 = HashFunctionDAO(4, 3, 7)
    List(h1, h2, h3)
  }

}
