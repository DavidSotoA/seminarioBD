import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object multiplicacionMatrices {
  def main(args: Array[String]): Unit = {
    val spark = initSparkSession
    val sc = spark.sparkContext
    val matA = createMatrix(args(2), sc, args(0).toInt, false)
    val matB = createMatrix(args(3), sc, args(1).toInt, true)
    val a = matA.persist(MEMORY_ONLY_SER)
    val b = matB.persist(MEMORY_ONLY_SER)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val result = multiply(a, b).toDF("i", "j", "value")
    result.write.mode(SaveMode.Overwrite).format("parquet").save(args(4))
  }

  def initSparkSession(): SparkSession = {
    SparkSession.builder()
     .appName("multiplicacionDeMatrices2")
     .enableHiveSupport()
     .getOrCreate()
  }

  def createMatrix(
    url: String,
    sc: SparkContext,
    bandas: Int,
    requireTranspose: Boolean): RDD[(Int, Iterable[Iterable[(Int, Int, Double)]])] = {

    val mat = sc.textFile(url, bandas)
    val matSplit = mat.map(_.split(" "))
    val matWithRowIndex = matSplit.zipWithIndex
    var matWithIndex = matWithRowIndex.map(x => x._1.zipWithIndex.map(t => (x._2.toInt, t._2.toInt, t._1.toDouble)))
                       .flatMap(v => v)

    val addBandIndex = (rdd: RDD[(Int, Iterable[(Int, Int, Double)])] ) =>
    rdd.mapPartitionsWithIndex{
      (index, list) => {
        list.map (x => (index, x._2))
     }
    }


    if(requireTranspose){
      val matGroup = matWithIndex.groupBy(_._2).sortBy(_._1)
      return addBandIndex(matGroup).groupByKey
    }
    val matGroup = matWithIndex.groupBy(_._1).sortBy(_._1)
    return addBandIndex(matGroup).groupByKey
  }

  def dotProduct(a: Iterable[(Int, Int, Double)], b: Iterable[(Int, Int, Double)]): Double = {
    a.zip(b).map(x => x._1._3*x._2._3).reduce(_+_)
  }

  def multiply(
    rddA: RDD[(Int, Iterable[Iterable[(Int, Int, Double)]])],
    rddB: RDD[(Int, Iterable[Iterable[(Int, Int, Double)]])]): RDD[(Int, Int, Double)] = {
    val rdd = rddA.cartesian(rddB)
    val result = rdd.map{x =>
                  (x._1)._2.map{x2 =>
                    (x._2)._2.map{x3 =>
                      (x2.toList(0)._1, x3.toList(0)._2, dotProduct(x2,x3))
                    }
                  }
                }
    result.flatMap(x => x.flatMap(y => y))
  }
}
