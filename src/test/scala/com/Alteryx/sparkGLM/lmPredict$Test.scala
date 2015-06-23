package com.Alteryx.sparkGLM

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import com.Alteryx.testUtils.data.testData._

class lmPredict$Test extends FunSuite {
  val sqlCtx = TestSQLContext

  test("lmPredict with a single partition") {
    val testDF = testDFSinglePart
    val x = testDF.select("intercept", "x")
    val y = testDF.select("y")
    val lmTest = LM.fit(x, y)
    val predicted = LM.predict(lmTest, x)

    assert(predicted.getClass.getName == "org.apache.spark.sql.DataFrame")
    assert(predicted.rdd.partitions.size == 1)
    assert(predicted.columns.size == 2)
    assert(predicted.agg(max("index")).collect.apply(0).get(0) == 49)
  }

  test("lmPredict with multiple partitions") {
    val testDF = testDFMultiPart
    val x = testDF.select("intercept", "x")
    val y = testDF.select("y")
    val lmTest = LM.fit(x, y)
    val predicted = LM.predict(lmTest, x)

    assert(predicted.getClass.getName == "org.apache.spark.sql.DataFrame")
    assert(predicted.rdd.partitions.length == 4)
    assert(predicted.columns.length == 2)
    assert(predicted.agg(max("index")).collect.apply(0).get(0) == 49)
  }
}
