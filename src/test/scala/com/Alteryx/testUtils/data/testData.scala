package com.Alteryx.testUtils.data

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame}
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

  val testRDD = TestSQLContext.sparkContext.parallelize(Seq(
      Array(1.0, 1.1, 21.4),
      Array(1.0, 2.2, 36.5),
      Array(1.0, 3.3, 15.0),
      Array(1.0, 4.4, 62.5),
      Array(1.0, 5.5, 36.1),
      Array(1.0, 6.6, 12.0),
      Array(1.0, 7.7, 37.0),
      Array(1.0, 8.8, 41.0),
      Array(1.0, 9.9, 36.6),
      Array(1.0, 11.0, 17.9),
      Array(1.0, 12.1, 53.1),
      Array(1.0, 13.2, 29.6),
      Array(1.0, 14.3, 8.3),
      Array(1.0, 15.4, -24.7),
      Array(1.0, 16.5, 41.0),
      Array(1.0, 17.6, 16.5),
      Array(1.0, 18.7, 16.0),
      Array(1.0, 19.8, 34.1),
      Array(1.0, 20.9, 30.5),
      Array(1.0, 22.0, 24.9),
      Array(1.0, 23.1, 30.3),
      Array(1.0, 24.2, 26.4),
      Array(1.0, 25.3, 11.2),
      Array(1.0, 26.4, -31.2),
      Array(1.0, 27.5, 19.9),
      Array(1.0, 28.6, 5.3),
      Array(1.0, 29.7, 2.2),
      Array(1.0, 30.8, -25.2),
      Array(1.0, 31.9, -6.5),
      Array(1.0, 33.0, 10.4),
      Array(1.0, 34.1, 28.1),
      Array(1.0, 35.2, -2.3),
      Array(1.0, 36.3, 6.5),
      Array(1.0, 37.4, -3.5),
      Array(1.0, 38.5, -31.0),
      Array(1.0, 39.6, -12.9),
      Array(1.0, 40.7, -13.6),
      Array(1.0, 41.8, -8.0),
      Array(1.0, 42.9, 14.1),
      Array(1.0, 44.0, 6.3),
      Array(1.0, 45.1, -13.4),
      Array(1.0, 46.2, -16.3),
      Array(1.0, 47.3, 1.6),
      Array(1.0, 48.4, -2.3),
      Array(1.0, 49.5, -28.3),
      Array(1.0, 50.6, -29.7),
      Array(1.0, 51.7, -9.4),
      Array(1.0, 52.8, -2.4),
      Array(1.0, 53.9, -21.1),
      Array(1.0, 55.0, -2.4)
  ), 4).map(x => Row(x(0), x(1), x(2)))

  val testSchema = StructType(
      StructField("intercept", DoubleType, true) ::
      StructField("x", DoubleType, true) ::
      StructField("y", DoubleType, true) :: Nil)

  val testDFSinglePart: DataFrame = {
    TestSQLContext.createDataFrame(testRDD, testSchema).coalesce(1)
  }

  val testDFMultiPart: DataFrame = {
    TestSQLContext.createDataFrame(testRDD, testSchema)
  }
}
