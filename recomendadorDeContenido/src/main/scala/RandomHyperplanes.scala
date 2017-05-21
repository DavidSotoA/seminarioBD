package com.lsh

import scala.util.Random

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class RandomHyperplanes(
    dataset_RH: Dataset[_],
    numHashTables_RH: Int,
    spark_RH: SparkSession) {
  val dataset = dataset_RH
  val numHashTables = numHashTables_RH
  val spark = spark_RH
  require(numHashTables > 0, "numHashTables debe ser mayor a cero")

  def lsh(colForLsh: String, colOutput: String): DataFrame = {
    val partiallyHashFunction = hashFunction( _ : Vector)
    val transformUDF = udf(partiallyHashFunction)
    val signatureDF = dataset.withColumn(colOutput,
      transformUDF(dataset(colForLsh)))
    signatureDF.repartition(col(colOutput))
  }

  def stringSignature(numBin: Array[Int]): String = {
   var stringSignature = ""
   for(i <- 0 to (numBin.length-1)) {
     stringSignature = stringSignature + numBin(i).toString
   }
   stringSignature
 }


  def hashFunction(instance: Vector): String = {
     val signature = (dotRestult: Double) => {
       if (dotRestult >= 0) {
         1
       } else {
         0
       }
     }

     val binSignature =  Array.fill(numHashTables) {
       signature(instance.toArray.map(_*Random.nextGaussian()).sum)
     }
     stringSignature(binSignature)
  }

}
