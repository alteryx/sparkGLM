package com.Alteryx.sparkGLM

import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.FunSuite
import com.Alteryx.testUtils.data.testData._

class modelMatrix$Test extends FunSuite {
  val sqlCtx = TestSQLContext

  test("modelMatrixWithMixedTypes") {
    val testDF = modelMatrix(dummyDF)
    assert(testDF.columns.length == 4)
    val expectedCols = Array("intField", "strField_b", "strField_c", "numField")
    assert(expectedCols.forall { elem =>
      testDF.columns.contains(elem)
    })
    assert(testDF.dtypes.forall(_._2 == "DoubleType"))
  }

  test("modelMatrixWithNumOnly") {
    val testDF = modelMatrix(dummyDF.select("numField", "intField"))
    assert(testDF.columns.length == 2)
    val expectedCols = Array("numField", "intField")
    assert(expectedCols.forall { elem =>
      testDF.columns.contains(elem)
    })
    assert(testDF.dtypes.forall(_._2 == "DoubleType"))
  }

  test("modelMatrixWithStrOnly") {
    val testDF = modelMatrix(dummyDF.select("strField"))
    assert(testDF.columns.length == 2)
    val expectedCols = Array("strField_b", "strField_c")
    assert(expectedCols.forall { elem =>
      testDF.columns.contains(elem)
    })
    assert(testDF.dtypes.forall(_._2 == "DoubleType"))
  }

  test("modelMatrixLinearRegData") {
    val rawDF = mixedDF.select("intercept", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "y")
    val testDF = modelMatrix(rawDF)
    assert(testDF.columns.length == 10)
    val expectedCols = Array("intercept", "x1", "x2", "x3", "x4", "x5", "x6", "x7_b", "x7_c", "y")
    assert(expectedCols.forall { elem =>
      testDF.columns.contains(elem)
    })
    assert(testDF.dtypes.forall(_._2 == "DoubleType"))
  }
}
