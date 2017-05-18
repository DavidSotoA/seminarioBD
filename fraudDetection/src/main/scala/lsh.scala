package com.minhash

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object Lsh {

  def lsh(numBands: Int, df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    // partir por bandas
    val dfColumns =  df.columns.drop(1)
    var queryStr = createQueryConcat(dfColumns, numBands)
    df.createOrReplaceTempView(Constants.MINHASH_TABLE)
    val dfByBands = spark.sql(queryStr)
    // concatenar bandas en una sola columna
    val dfColumnsBands = dfByBands.columns.drop(1)
    queryStr = queryBandsToArray(dfColumnsBands)
    dfByBands.createOrReplaceTempView("bandsTable")
    var dfSignature = spark.sql(queryStr)
    // convertir signatures a lista
    dfSignature =
    dfSignature.map(row =>
      (row(0).asInstanceOf[Int],
      row(1).asInstanceOf[String].split(" "))
    ).toDF(Constants.INDEX_COLUMN, Constants.SIGNATURE_COLUMN)
    // realizar explode
    dfSignature = dfSignature
                  .select(dfSignature(Constants.INDEX_COLUMN),
                   explode(dfSignature(Constants.SIGNATURE_COLUMN))
                  .as(Constants.SIGNATURE_COLUMN))
   // agrupar documentos por firma
   val groupDocs = new GroupDocs()
   dfSignature = dfSignature
   .groupBy(Constants.SIGNATURE_COLUMN)
   .agg(groupDocs(dfSignature(Constants.INDEX_COLUMN))
   .as(Constants.SIMILARITY_COLUMN))
   .drop(Constants.SIGNATURE_COLUMN)
   .distinct

   dfSignature.filter(x => x(0).asInstanceOf[Seq[Int]].size>1)
  }

  def queryBandsToArray(columnNames: Array[String]): String = {
    val numOfColumns = columnNames.length
    var queryStr = "SELECT " + Constants.INDEX_COLUMN + ", concat("
    for(i <- 0 until numOfColumns) {
      val band = columnNames(i)
      queryStr = queryStr + band + ", ' ', "
    }
    queryStr.dropRight(7) + ") as " + "signatures FROM " + Constants.BANDS_TABLE
  }

  def createQueryConcat(columnNames: Array[String], numeroDeBandas: Int): String = {
    val numOfColumns = columnNames.length
    val minhashPorBandaCompletos = numOfColumns / numeroDeBandas
    val minhashPorBandaIncompletos = numOfColumns % numeroDeBandas

    val concatStr = (bands: Array[String], index: Int, colName: String) => {
      var str = "concat("
      for (band <- bands) {
        str = str + band + ", "
      }
      str = str + "'" + index + "'"+ ") as " + colName
      str
    }

    val colNameStr = (index: Int) => "band" +  index
    var queryStr = "SELECT " + Constants.INDEX_COLUMN + ", "
    var i = 0
    var numOfBands = 0
    var index = 1
    var delta = minhashPorBandaCompletos
    while (i < numOfColumns) {
      if(numOfBands == numeroDeBandas - 1) {
        delta = delta + minhashPorBandaIncompletos
      }
      var bands = columnNames.slice(i, (i + delta))
      val colname = colNameStr(index)
      queryStr = queryStr + concatStr(bands, index, colname) + ", "
      numOfBands = numOfBands + 1
      i = i + delta
      index = index + 1
    }
    queryStr.dropRight(2) + " FROM " + Constants.MINHASH_TABLE
  }
}

class GroupDocs() extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Array(
   StructField("doc", IntegerType)
  ))

  override def bufferSchema: StructType =
    StructType(Array(
      StructField("docs", ArrayType(IntegerType))
    ))

  override def dataType: DataType = ArrayType(IntegerType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
   buffer(0) = Array[Int]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
     buffer(0) = buffer(0).asInstanceOf[Seq[Int]] :+ input.getInt(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1(0).asInstanceOf[Seq[Int]] ++ buffer2(0).asInstanceOf[Seq[Int]]
  }

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}
