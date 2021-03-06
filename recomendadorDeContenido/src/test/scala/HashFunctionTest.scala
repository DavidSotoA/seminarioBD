package com.test

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.recommendationSys.Constants

class HashFunctionTest extends FunSuite{


  test("No se generan colisions entre las funciones hash"){
    val df = spark.createDataFrame(Seq(
     (1, 1, 1.5),
     (1, 2, 1.5),
     (1, 3, 1.5),
     (1, 4, 0.5),
     (1, 5, -2.5),
     (1, 6, -2.5),
     (1, 7, 0.0),
     (2, 1, 0.833),
     (2, 2, -0.167),
     (2, 3, -0.167),
     (2, 4, 0.0),
     (2, 5, 0.833),
     (2, 6, -2.167),
     (2, 7, 0.833),
     (3, 1, 0.4),
     (3, 2, 0.4),
     (3, 3, 0.4),
     (3, 4, 0.0),
     (3, 5, 0.0),
     (3, 6, -0.6),
     (3, 7, -0.6),
     (4, 1, -1.5),
     (4, 2, 0.5),
     (4, 3, 1.5),
     (4, 4, 0.0),
     (4, 5, 0.5),
     (4, 6, -0.5),
     (4, 7, -0.5),
     (5, 1, 0.833),
     (5, 2, -1.167),
     (5, 3, -1.167),
     (5, 4, 0.0),
     (5, 5, 0.833),
     (5, 6, -0.167),
     (5, 7, 0.833),
     (6, 1, 0.667),
     (6, 2, -0.333),
     (6, 3, -0.333),
     (6, 4, 0.0),
     (6, 5, 0.0),
     (6, 6, 0.0),
     (6, 7, 0.0))
   ).toDF(Constants.COL_USER_ID,
          Constants.COL_MOVIE_ID,
          Constants.COL_RATING)
  }

  val dfMovies = spark.createDataFrame(Seq(
   (1, "toy story", "teror"),
   (2, "Carpool", "drama"),
   (3, "Jumanji", "romance"),
   (4, "Dumbo", "romance"),
   (5, "Sabrina", "erotica"),
   (6, "jude", "erotica"),
   (7, "El excorsista", "infantil"))
 ).toDF(Constants.COL_MOVIE_ID,
        Constants.COL_NAME_MOVIE,
        Constants.COL_GENRES)

  val df = spark.createDataFrame(Seq(
   (1, 1, 1.5),
   (1, 2, 1.5),
   (1, 3, 1.5),
   (1, 4, 0.5),
   (1, 5, -2.5),
   (1, 6, -2.5),
   (2, 1, 0.833),
   (2, 2, -0.167),
   (2, 3, -0.167),
   (2, 5, 0.833),
   (2, 6, -2.167),
   (2, 7, 0.833),
   (3, 1, 0.4),
   (3, 2, 0.4),
   (3, 3, 0.4),
   (3, 6, -0.6),
   (3, 7, -0.6),
   (4, 1, -1.5),
   (4, 2, 0.5),
   (4, 3, 1.5),
   (4, 5, 0.5),
   (4, 6, -0.5),
   (4, 7, -0.5),
   (5, 1, 0.833),
   (5, 2, -1.167),
   (5, 3, -1.167),
   (5, 5, 0.833),
   (5, 6, -0.167),
   (5, 7, 0.833),
   (6, 1, 0.667),
   (6, 2, -0.333),
   (6, 3, -0.333))
 ).toDF(Constants.COL_USER_ID,
        Constants.COL_MOVIE_ID,
        Constants.COL_RATING)
}

}
