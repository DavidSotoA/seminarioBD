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

 /** Método crear el dataframe a partir del cual se le recomienda a los usuarios
 *  @param pathRanking: dirección del archivo con los datos de ranking
 *  (descargado de https://grouplens.org/datasets/movielens/latest/)
 *  Los datos tienen la siguiente estructura
 *   _______________________________
 *  |userId|movieId|rating|timestamp|
 *  |1     |31     |2.5   |4512212  |
 *  |______|_______|______|_________|
 *  @param pathMovies: dirección del archivo con los datos de las peliculas
 *  @param spark: SparkSession
 *  @param pathSave: dirección donde se guardara el dataframe
 *  @return no retorna nada, el método debe de guardar el dataframe preprocesado en el HDFS
 */
  def createMatrizForRecommendMovies(
    pathRanking: String,
    pathMovies: String,
    spark: SparkSession): DataFrame = {
      val rankingDf = spark.read.load(pathRanking)
      val moviesDf = spark.read.load(pathMovies)
      val normalizedDf = normalized(rankingDf)
      val lshDf = lsh(normalizedDf, moviesDf, spark)
      lshDf
  }

  /** Método encargado de normalizar los datos
  *  @param path: datframe con os rankings de las peliculas
  *  @return dataframe con los datos normalizados
  */
  def normalized(rankingDf: DataFrame): DataFrame = {
    val df = rankingDf.select(expr("(split('value', ','))[0]").cast("string").as(Constants.COL_USER_ID), expr("(split('value', ','))[1]").cast("string").as(Constants.COL_MOVIE_ID), expr("(split('value', ','))[2]").cast("double").as(Constants.COL_RATING))
    val dfxUser = df.groupBy(Constants.COL_USER_ID).agg(avg(df(Constants.COL_RATING)).as("promedio"))
    df.join(dfxUser,Constants.COL_USER_ID).withColumn(Constants.COL_RATING,df(Constants.COL_RATING)-dfxUser("promedio")).drop(df(Constants.COL_RATING)).drop(dfxUser("promedio"))
  }

  /** Método encargado de asignar los usuario a cubetas
  *  @param df: Dataframe con los datos normalizados
  *  @param dfMovies: Dataframe de las peliculas
  *   ____________________________________________________________________
  *  |movieId|title           |genres                                     |
  *  |1      |Toy Story (1995)|Adventure|Animation|Children|Comedy|Fantasy|
  *  |_______|________________|___________________________________________|
  *  @param spark: SparkSession
  *  @return dataframe con los usuarios asignados en cubetas
  */
  def lsh(df: DataFrame, dfMovies: DataFrame, spark: SparkSession): DataFrame = {
    val createFeatures =  new CreateFeatures()
    val featuresDf = df.groupBy(Constants.COL_USER_ID).agg(createFeatures(df(Constants.COL_MOVIE_ID), df(Constants.COL_RATING)).as(Constants.COL_FEATURES))
    val featuresIndex = dfMovies.select(Constants.COL_MOVIE_ID).collect
    val featuresIndexFlatten = featuresIndex.map(_(0).asInstanceOf[Int])
    val randomHyperplanes = new RandomHyperplanes(featuresDf, featuresIndexFlatten, 3, spark)
    val signatureDf = randomHyperplanes.lsh(Constants.COL_FEATURES, Constants.COL_SIGNATURE).drop(Constants.COL_FEATURES)
    df.join(signatureDf, Constants.COL_USER_ID).drop(Constants.COL_FEATURES)
  }

  /** Método encargado de asignar los usuario a cubetas
  *  @param user: Id del usuario al que se le recomendara contenido
  *  @param df: dataframe con cubetas
  *  @param cant: número de peliculas que se le recomendara al usuario
  *  @return array con los ids de las peliculas recomendadas
  */
  def recommendToUser(user: Int, df: DataFrame, cant: Int): Array[Int] = {
    val infoUser = df.select(Constants.COL_MOVIE_ID, Constants.COL_SIGNATURE).where(Constants.COL_USER_ID + " == " + user)
    val signature = infoUser.head()(1).asInstanceOf[String]
    var neighborsDf = df.select(Constants.COL_MOVIE_ID,Constants.COL_RATING).where(Constants.COL_SIGNATURE + " == "+ signature + " and " + Constants.COL_USER_ID + " != " + user)
    neighborsDf = neighborsDf.join(infoUser, Seq(Constants.COL_MOVIE_ID), "leftanti")
    neighborsDf.groupBy(Constants.COL_MOVIE_ID).avg(Constants.COL_RATING).orderBy(desc("avg(rating)")).drop("avg(rating)").head(cant).map(x => x(0).asInstanceOf[Int])
  }
}

//UDAF encargada de colocar las calificaciones de los usuarios como un vector de caracteristicas
class CreateFeatures() extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Array(
   StructField("movieId", IntegerType),
   StructField("ranking", DoubleType)
  ))

  override def bufferSchema: StructType =StructType(Array(
     StructField("allInfo", ArrayType(StructType(Array(
                                       StructField("movieId", IntegerType),
                                       StructField("ranking", DoubleType)
                                     ))))
                                   ))


  override def dataType: DataType = ArrayType(StructType(Array(
                                     StructField("movieId", IntegerType),
                                     StructField("ranking", DoubleType)
                                    )))

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[(Int, Double)]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer(0).asInstanceOf[Seq[(Int, Double)]] :+ (input.getInt(0), input.getDouble(1))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1(0).asInstanceOf[Seq[(Int, Double)]] ++
                 buffer2(0).asInstanceOf[Seq[(Int, Double)]]
  }

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }

}
