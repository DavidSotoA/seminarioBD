package com.minhash

import scala.util.control.Breaks._
//"/home/skorponx/2017-1/seminarioBD/minhash/primos.txt"

object HashFunction {

  def createHashFunctions(
    maxValue: Int,
    numOfHashFunctions: Int,
    dirPrimes: String): (Int, List[(Int, Int)]) = {
      val lastNum = maxValue*(numOfHashFunctions + 50) + (numOfHashFunctions + 50)
      require((lastNum <= Constants.LAST_PRIME),
      "El numero primo requerido (mayor a" + lastNum + ")no esta disponible")
      require((numOfHashFunctions <= 100), ";)")

      val primList = scala.io.Source.fromFile(dirPrimes).getLines.toList
      val prime = findPrimeNumber(lastNum, primList)
      var valuesAandB = List[(Int,Int)]()
      val r = scala.util.Random
      var i = 0
      while(i < numOfHashFunctions) {
        val m = r.nextInt(numOfHashFunctions + 50) + 1
        val b = r.nextInt(numOfHashFunctions + 50) + 1
        var correctValues = true

        breakable {
          for(function <- valuesAandB) {
            val (mi, bi) = function
            val intersection = (b - bi).toDouble/(m - mi)

            if (((intersection % 1) == 0.0)  || (bi == b)) {
              correctValues = false
              break
            }
          }
        }

        if(correctValues) {
          i = i + 1
          valuesAandB = valuesAandB :+ (m, b)
        }
      }
      (prime, valuesAandB)
    }

  def findPrimeNumber(maxValue: Int, primeList: List[String]): Int = {
    val valueList = primeList.find(_.toInt>maxValue)
    return valueList.toList(0).toInt
  }

  def evaluateFunction(a: Int, x: Int, b: Int, m: Int): Int = {
    return (a*x + b) % m
  }

  def evaluateHashFunctions(hashFunctions: (Int, List[(Int, Int)]), value: Int): List[Int] = {
    var hashVal = List[Int]()
    val (module, valuesAandB) = hashFunctions
    for(a_and_b <- valuesAandB) {
        val (a, b) = a_and_b
        hashVal = hashVal :+ evaluateFunction(a, value, b, module)
    }
    hashVal
  }

}
