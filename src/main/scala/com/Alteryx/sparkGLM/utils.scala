package com.Alteryx.sparkGLM

import breeze.linalg._
import breeze.numerics._

import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType

object utils {

  def matchCols (estDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    val missingCols: Array[Column] = estDF.columns.diff(scoreDF.columns).map { field =>
      lit(0).cast(DoubleType).as(field)
    }
    scoreDF.select(missingCols :+ col("*"):_*)
  }

  // HOLDING ON TO THIS FOR NOW, IT DEPENDS IF THINGS MAKE IT ACROSS FROM mlmatrix
  def dataFrameToMatrix(df: DataFrame): RDD[DenseMatrix[Double]] = {
    val matrixRDD = df.map(x => x.toSeq.toArray).map(y => y.map(z => z.asInstanceOf[Double]))
    RowPartitionedMatrix.arrayToMatrix(matrixRDD)
  }

  // Convert a single partitioned DataFrame into a DenseMatrix
  def dfToDenseMatrix(df: DataFrame): DenseMatrix[Double] = {
    require(df.rdd.partitions.size == 1,
      "The DataFrame must be in a single partition")
    require(df.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    val dataArray = dataFrameToMatrix(df).collect
    dataArray(0)
  }

  // From ml-matrix, the reducer function to sum together the partitons of X'X
  // and X'y
  def reduceNormal(
    a: (DenseMatrix[Double], DenseMatrix[Double]),
    b: (DenseMatrix[Double], DenseMatrix[Double])): (DenseMatrix[Double], DenseMatrix[Double]) = {
    a._1 :+= b._1
    a._2 :+= b._2
    a
  }
}
