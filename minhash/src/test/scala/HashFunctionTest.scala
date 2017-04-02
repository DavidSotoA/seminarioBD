package com.test

import com.minhash.HashFunction
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class HashFunctionTest extends FunSuite{


  test("No se generan colisions entre las funciones hash"){
    val hashFunctions = HashFunction.createHashFunctions(1000,
    100,"/home/skorponx/2017-1/seminarioBD/minhash/primos.txt")

    for(i <- 1 to 1000) {
      val valHashs = HashFunction.evaluateHashFunctions(hashFunctions, i)
      val distincts = valHashs.distinct
      assert(valHashs.size == distincts.size)
    }
  }

}
