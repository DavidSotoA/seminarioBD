package com.lsh

import scala.util.Random

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class RandomHyperplanes(
    dataset_RH: Dataset[_],
    indexFeatures: Array[Int],
    numHashTables_RH: Int,
    spark_RH: SparkSession) {
  val dataset = dataset_RH
  val numHashTables = numHashTables_RH
  val spark = spark_RH
  require(numHashTables > 0, "numHashTables debe ser mayor a cero")
  var hyperplanes = createHiperplanes(numHashTables, indexFeatures)

  def lsh(colForLsh: String, colOutput: String): DataFrame = {
    val partiallyHashFunction = hashFunction( _ : Seq[Row], hyperplanes)
    val transformUDF = udf(partiallyHashFunction)
    val signatureDF = dataset.withColumn(colOutput, transformUDF(dataset(colForLsh)))
    signatureDF.repartition(col(colOutput))
  }

  def createHiperplanes(numHashTables: Int, indexFeatures: Array[Int]): Array[Array[(Int, Double)]] = {
    val inputDim = indexFeatures.size
    var h = Array.fill(numHashTables) {Array.fill(inputDim)(Random.nextGaussian())}
    h.map(x => indexFeatures zip x )
  }

  def stringSignature(numBin: Array[Int]): String = {
   var stringSignature = ""
   for(i <- 0 to (numBin.length-1)) {
     stringSignature = stringSignature + numBin(i).toString
   }
   stringSignature
 }

def hashFunction(instance: Seq[Row],
   hashFunctions: Array[Array[(Int, Double)]]): String = {
     val signature = (dotRestult: Double) => {
       if (dotRestult >= 0) {
         1
       } else {
         0
       }
     }

    val binSignature = hashFunctions.map(hash => signature(dot(hash, instance)))
    stringSignature(binSignature)
  }

  def dot(hash: Array[(Int, Double)], instance: Seq[Row]): Double = {
    var index = 0
    var dot = Seq[Double]()
    for (i <- 0 until instance.size) {
      val indHyperplane = hash(i)._1
      val indUser = instance(index)(0).asInstanceOf[Int]
        if (indUser == indHyperplane) {
          dot = dot :+ (hash(i)._2 * instance(index)(1).asInstanceOf[Double]).toDouble
          index = index + 1
        }
      }
    dot.sum
  }

}
