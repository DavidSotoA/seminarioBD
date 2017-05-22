package com.recommendationSys

import com.lsh.RandomHyperplanes

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object RecommendationSystem {

 /** Método encargado de preprocesar el dataframe para el recomendador de contenido
 *  @param pathRanking: dirección con los datos de ranking
 *  (descargado de https://grouplens.org/datasets/movielens/latest/)
 *  Los datos tienen la siguiente estructura
 *   _______________________________
 *  |userId|movieId|rating|timestamp|
 *  |1     |31     |2.5   |4512212  |
 *  |______|_______|______|_________|
 *  @param spark: SparkSession
 *  @param pathSave: dirección donde se guardara el dataframe
 *  @return no retorna nada, el método debe de guardar el dataframe preprocesado en el HDFS
 */
  def preprocess(pathRanking: String, spark: SparkSession, pathSave: String) {
    throw new IllegalArgumentException ("unimplement method")
  }

  /** Método encargado de normalizar los datos
  *  @param path: direccion donde esta el archivo que almacena los datos
  *  @param spark: SparkSession
  *  @return dataframe con los datos normalizados
  */
  def normalize(path: String, spark: SparkSession): DataFrame = {
    val df = spark.read.format("text").load(path).select(expr("(split(value, ','))[0]").cast("string").as(Constants.COL_USER_ID), expr("(split(value, ','))[1]").cast("string").as(Constants.COL_MOVIE_ID), expr("(split(value, ','))[2]").cast("double").as(Constants.COL_RATING))
    val dfxUser = df.groupBy(Constants.COL_USER_ID).agg(avg(df(Constants.COL_RATING)).as("promedio"))
    df.join(dfxUser,Constants.COL_USER_ID).withColumn(Constants.COL_RATING,df(Constants.COL_RATING)-dfxUser("promedio")).drop(df(Constants.COL_RATING)).drop(dfxUser("promedio"))
  }


 //////////////////////////ESTO ES LO MIO///////////////////////////////////////
  /** Método encargado de asignar los usuario a cubetas
  *  @param df: Dataframe con los datos normalizados
  *  @param spark: SparkSession
  *  @return dataframe con los usuarios asignados en cubetas
  */
  def lsh(df: DataFrame, spark: SparkSession): DataFrame = {
    val createFeatures =  new CreateFeatures()
    val featuresDf = df.groupBy(Constants.COL_USER_ID).agg(createFeatures(df(Constants.COL_RATING)).as(Constants.COL_FEATURES))
    val randomHyperplanes = new RandomHyperplanes(featuresDf, Constants.AND_FUNCTIONS, spark)
    val signaturDf = randomHyperplanes.lsh(Constants.COL_FEATURES, Constants.COL_SIGNATURE).drop(Constants.COL_FEATURES)
    signaturDf.join(df, Constants.COL_USER_ID).drop(Constants.COL_FEATURES)
  }

  /** Método encargado de asignar los usuario a cubetas
  *  @param user: Id del usuario al que se le recomendara contenido
  *  @param df: dataframe
  *  @param cant: número de peliculas que se le recomendara al usuario
  *  @return array con los ids de las peliculas recomendadas
  */
  def recommendToUser(user: Int, df: DataFrame, cant: Int): Array[Int] = {
    val infoUser = df.select(Constants.COL_MOVIE_ID, Constants.COL_SIGNATURE).where(Constants.COL_USER_ID + " == " + user)
    val signature = infoUser.head()(0).asInstanceOf[Int]
    var neighborsDf = df.select(Constants.COL_MOVIE_ID,Constants.COL_RATING).where(Constants.COL_SIGNATURE + " == "+ signature + " and " + Constants.COL_USER_ID + " != " + user)
    neighborsDf = neighborsDf.join(infoUser, Seq(Constants.COL_MOVIE_ID), "leftanti")
    neighborsDf.groupBy(Constants.COL_MOVIE_ID).avg(Constants.COL_RATING).orderBy(desc("avg(rating)")).drop("avg(rating)").head(cant).map(x => x(0).asInstanceOf[Int])
  }
}

//UDAF encargada de colocar las calificaciones de los usuarios como un vector de caracteristicas
class CreateFeatures() extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Array(
   StructField("ranking", DoubleType)
  ))

  override def bufferSchema: StructType =
    StructType(Array(
      StructField("features", ArrayType(DoubleType))
    ))

  override def dataType: DataType = VectorType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Double]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer(0).asInstanceOf[Seq[Double]] :+ input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1(0).asInstanceOf[Seq[Double]] ++
                 buffer2(0).asInstanceOf[Seq[Double]]
  }

  override def evaluate(buffer: Row): Any = {
    Vectors.dense(buffer(0).asInstanceOf[Seq[Double]].toArray)
  }

}
