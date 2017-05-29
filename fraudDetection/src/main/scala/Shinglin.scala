package com.minhash

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD

import scala.Predef._
import org.apache.spark.SparkContext._
import java.io._


object Shinglin {

  def shinglin(booksPath: String,
    shinglinUrl: String,
    documentosUrl: String,
    stopWordsUrl: String,
    shinglinSize: Int,
    spark: SparkSession){

      //booksPath: Dirrecion de la carpeta donde estan los libros
      //shinglinUrl: Direccion donde se guardara la matriz con los resultados
      //documentosUrl: Direccion documentos con su id
      //stopWordsUrl: Dirrecion donde esta el archivos con las stop words
      //shinglinSize: Tamaño de las frases

      val sc = spark.sparkContext


      val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val schema = StructType(
        StructField("i", IntegerType, false) ::
        StructField("j", IntegerType, false) :: Nil)
        val schemaIn = StructType(
          StructField("url", StringType, false) :: Nil)

          val schemaLectura = StructType(
            StructField("nombre", StringType, false) ::
            StructField("contenido", StringType, false) ::
            StructField("i", IntegerType, false) :: Nil
          )

          var matrizResultado = try { spark.sqlContext.read.parquet(shinglinUrl) } catch { case e: Exception => spark.createDataFrame(sc.emptyRDD[Row], schema)}

          var matrizDocumentos = try { spark.sqlContext.read.parquet(documentosUrl) } catch { case e: Exception => spark.createDataFrame(sc.emptyRDD[Row], schema)}

          val documento = try{matrizResultado.select("j").orderBy(col("j").desc).limit(1).head.getDouble(0).toInt } catch{ case e: Exception => 0 }

          val libros = sc.wholeTextFiles(booksPath).zipWithIndex.map{x => (x._1._1,x._1._2,x._2+documento+1)}

          var dfFiles = libros.toDF("nombre","contenido","i")

          val dfDocumentos = dfFiles.drop("contenido")

          val tablaReferencia = dfFiles.select("nombre","i")

          dfFiles = dfFiles.drop("nombre")

          val stop = spark.read.format("text").load(stopWordsUrl).collect.map(_.mkString).toArray
          val k = shinglinSize;

          val abrir = udf { (s: String) => s.split(" ")}
          val limpieza = udf { (s: String) =>  s.toLowerCase.replaceAll("[^\\w\\s]", "").replaceAll("\\s+", " ")}
          val myLength=udf { (a: Seq[String]) => a.length}
          val contenido =udf {(a: String)=> stop contains a}
          val udfhash = udf {(a: Seq[String]) => a.map( x => myhashString(x.map( y => y.toInt).mkString)).mkString }

          val dfSeparado = dfFiles.withColumn("Array",abrir(col("contenido"))).drop("contenido").withColumn("palabras",explode(col("Array"))).drop("Array").withColumn("palabrasLimpias",limpieza(col("palabras"))).drop("palabras")
          var windowSpec = Window.partitionBy("i").orderBy("i").rowsBetween(0,k-1)
          val dfFrases = dfSeparado.withColumn("result", when(contenido(col("palabrasLimpias")),collect_list(col("palabrasLimpias")).over(windowSpec)).otherwise(Array[String]())).drop("palabrasLimpias").filter(myLength(col("result")) === k).withColumn("j",udfhash(col("result"))).drop("result").groupBy("i").agg(collect_set(col("j"))).withColumn("j",explode(col("collect_set(j)"))).drop("collect_set(j)")
          val matrizResultadoFinal = matrizResultado.union(dfFrases)
          
          val matrizDocumentosFinal = matrizDocumentos.union(dfDocumentos)

          matrizResultadoFinal.write.mode(SaveMode.Overwrite).format("parquet").save(shinglinUrl)
          matrizDocumentosFinal.write.mode(SaveMode.Overwrite).format("parquet").save(documentosUrl)

        }

        def myhashString(x: String) : BigInt = {

          try {
            val hola = BigInt(x)
            (472 * hola + 541)%BigInt("170141183460469231731687303715884105727")
          } catch {
            case e: Exception => 0
          }
        }
      }
