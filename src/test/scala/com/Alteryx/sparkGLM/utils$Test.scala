package com.Alteryx.sparkGLM

import org.scalatest.FunSuite
import org.apache.spark.sql.test.TestSQLContext
import com.Alteryx.testUtils.data.testData._

class utils$Test extends FunSuite {
  val sqlCtx = TestSQLContext

  test("matchCols") {
    val df = modelMatrix(dummyDF)
    val dfWithMissingCategory = modelMatrix(oneLessCategoryDF)

    val testDF = utils.matchCols(df, dfWithMissingCategory)
    assert(testDF.getClass.getName == "org.apache.spark.sql.DataFrame")
    assert(testDF.columns.length == 4)
    assert(testDF.dtypes.forall(_._2 == "DoubleType"))
    val expectedCols = Array("intField", "strField_b", "strField_c", "numField")
    assert(expectedCols.forall { elem =>
      testDF.columns.contains(elem)
    })
    assert(testDF.select("strField_c").distinct.count == 1)
    assert(testDF.select("strField_c").distinct.collect().apply(0).get(0) === 0)
  }
}
