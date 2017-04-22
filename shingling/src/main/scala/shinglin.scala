package main

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.Row


object multiplicarMatricesBandas {


  def main(args: Array[String]){

    //args(0): direccion de la primera matriz
    //args(1): direccion de la segunda matriz
    //args(3): numero de bandas

    val spark = SparkSession.builder()
    .appName("Multiplicacion de Matrices, por bandas")
    .enableHiveSupport()
    .getOrCreate()

    val sc = spark.sparkContext

    val schema = StructType(
      StructField("i", DoubleType, false) ::
      StructField("j", DoubleType, false) :: Nil)

      var matrizResultado = try { spark.sqlContext.read.parquet(args(0)) } catch { case e: Exception => spark.createDataFrame(sc.emptyRDD[Row], schema)}


      val abrir = udf { (s: String) => s.split(" ")}


      val dfA = spark.read.format("text").load(args(1))
      val stop = spark.read.format("text").load("/user/bigdata1-1701/stopwords.txt").collect.map(_.mkString).toArray
      val dfSeparado = dfA.withColumn("Array",abrir(dfA("value"))).withColumn("palabras",explode(col("Array"))).drop("value").drop("Array")
      val posiblesFrases = dfSeparado.collect.map(_.mkString).toList.map(_.toLowerCase.replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ")).sliding(3).map(_.mkString(" ")).toList.distinct.filterNot(row => row.split(" ").length < 1)
      val dffrasesStop = posiblesFrases.filterNot(row => stop contains row.split(" ")(0))

      val documento = try{matrizResultado.select("j").orderBy(matrizResultado("j").desc).limit(1).head.getDouble(0).toInt } catch{ case e: Exception => 0 }

      val representacionhas = dffrasesStop.map(row => myhash(row.map(_.toInt).reduceLeft(_+_))).distinct

      val rows = spark.createDataFrame(representacionhas.toSeq).toDF("i").withColumn("j", lit(documento)

      matrizResultado = matrizResultado.union(rows)

      matrizResultado.write.mode(SaveMode.Overwrite).format("parquet").save(args(0))

    }

    def myhash(x: Int) : Int = {
      (472 * x + 541)%2147483647
    }
  }
