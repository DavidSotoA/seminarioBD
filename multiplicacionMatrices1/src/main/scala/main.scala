import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Tokenizer

object multiplicacionMatrices {
  def main(args: Array[String]): Unit = {
    val spark = initSparkSession
    val matA = createMatrix(args(0), ("M_values", "i", "j"), spark)
    val matB = createMatrix(args(1),("N_values", "j", "k"), spark)
    val multMat = mult(matA, matB)
    val multMatRenamed = multMat.toDF("i", "j", "values")
    multMatRenamed.write.mode(SaveMode.Overwrite).format("parquet").save(args(2))
  }

  def createMatrix(url: String, names: (String, String, String ), spark: SparkSession): DataFrame = {
    val matTxt = spark.read.format("text").load(url)
    val tokenizer = new Tokenizer().setInputCol("value").setOutputCol("words")
    val tokenized = tokenizer.transform(matTxt).drop("value")
    val matWithRowsId = tokenized.withColumn(names._2, monotonicallyIncreasingId + 1)
    val matExplode = matWithRowsId.select(explode(matWithRowsId("words")).as("value"), matWithRowsId(names._2))

    val castToDouble: String => Double = _.toDouble
    val toDoubleUDF = udf(castToDouble)
    val matDouble = matExplode.withColumn(names._1, toDoubleUDF(matExplode("value"))).drop("value")

    matDouble.persist(MEMORY_ONLY_SER)
    val numOfCols = matDouble.select(names._2).where(names._2 + " == 1").count
    matDouble.withColumn(names._3, (monotonicallyIncreasingId % numOfCols) + 1)
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder()
     .appName("multiplicacionDeMatrices")
     .enableHiveSupport()
     .getOrCreate()
  }

  def mult(matA: DataFrame, matB: DataFrame): DataFrame = {
    val joinMat = matA.join(matB, matA("j") === matB("j")).drop(matA("j"))
    val multiplyValues= (a: Double, b: Double) => a*b
    val multiplyValuesUDF = udf(multiplyValues)
    // broadcast a matB?
    val matMultiplyValues = joinMat.withColumn("M_jxN_j", multiplyValuesUDF(joinMat("M_values"),
                                    joinMat("N_values"))).drop("M_values", "N_values")
    matMultiplyValues.groupBy("i","k").sum("M_jxN_j")
  }

}
