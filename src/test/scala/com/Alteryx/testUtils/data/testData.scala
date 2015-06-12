package com.Alteryx.testUtils.data

import org.apache.spark.sql.{DataFrame, SQLContext}


object testData {
  def numericDF(sqlCtx: SQLContext): DataFrame = {
    sqlCtx.load("./src/test/scala/com/Alteryx/testUtils/data/linear_reg_all_numeric.json", "json")
  }

  def mixedDF(sqlCtx: SQLContext): DataFrame = {
    sqlCtx.load("./src/test/scala/com/Alteryx/testUtils/data/linear_reg_mixed.json", "json")
  }

  case class testRow(intField: Int, strField: String, numField: Double)
  def dummyDF(sqlCtx: SQLContext): DataFrame = {
    import sqlCtx.implicits._

    sqlCtx.sparkContext.parallelize(
      testRow(1, "a", 1.0) ::
      testRow(2, "b", 2.0) ::
      testRow(3, "c", 3.0) :: Nil).toDF()
  }
}
