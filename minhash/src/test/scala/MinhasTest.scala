package com.test


import com.minhash.HashF}unction
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class MinshashTest extends FunSuite{

  test("test") {
    val instances = spark.createDataFrame(Seq(
      (0, 2),
      (0, 4),
      (0, 5),
      (1, 2),
      (1, 5),
      (2, 1),
      (2, 4),
      (3, 3),
      (3, 6),
      (4, 3),
      (4, 4),
      (4, 5),
      (4, 6),
      (5, 1),
      (5, 5),
      (6, 2),
      (6, 3),
      (6, 4)
    )).toDF("i", "j")
  }
}
