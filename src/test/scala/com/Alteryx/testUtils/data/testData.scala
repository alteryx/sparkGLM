package com.Alteryx.testUtils.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test._
import org.apache.spark.sql.test.TestSQLContext.implicits._


object testData {
  val numericDF: DataFrame = TestSQLContext.read.json(
    "./src/test/scala/com/Alteryx/testUtils/data/linear_reg_all_numeric.json")

  val mixedDF: DataFrame = TestSQLContext.read.json(
    "./src/test/scala/com/Alteryx/testUtils/data/linear_reg_mixed.json")

  case class testRow(intField: Int, strField: String, numField: Double)
  val dummyDF: DataFrame = {
    TestSQLContext.sparkContext.parallelize(
      testRow(1, "a", 1.0) ::
      testRow(2, "b", 2.0) ::
      testRow(3, "c", 3.0) :: Nil).toDF()
  }

  val oneLessCategoryDF: DataFrame = {
    TestSQLContext.sparkContext.parallelize(
      testRow(1, "a", 1.0) ::
      testRow(2, "b", 2.0) ::
      testRow(3, "a", 3.0) :: Nil).toDF()
  }
}
