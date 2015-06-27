package com.Alteryx.sparkGLM

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._

import org.apache.commons.math3.distribution._

import edu.berkeley.cs.amplab.mlmatrix
import edu.berkeley.cs.amplab.mlmatrix._
import edu.berkeley.cs.amplab.mlmatrix.{NormalEquations, RowPartitionedMatrix}
import edu.berkeley.cs.amplab.mlmatrix.util.Utils
import edu.berkeley.cs.amplab.mlmatrix.NormalEquations._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions

case class GLM(xnames: Array[String],
              yname: String,
              coefs: DenseMatrix[Double],
              stdErr: Array[Double],
              dfResidual: Double,
              dfNull: Double,
              deviance: Double,
              nullDeviance: Double,
              pDispersion: Double,
              pearson: Double,
              loglik: Double,
              family: String,
              link: String,
              aic: Double,
              iter: Int)

object GLM {

  // Family and link methods
  /// Family specific methods
  //// Binomial
  def varianceBinomial(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    mu :* (1.0 :+ (-1.0 :* (mu :/ m)))
  }

// NEARLY THERE, THE ISSUE IS APPLYING A FUNCTION TO ELEMENTS OF
// DIFFERENT COLUMNS TO GET THE CDF VALUE. HERE IS A RELEVANT
// CODE SNIPPET:
// val a = Binomial(1, 0.5).logProbabilityOf(1)
// this is of the form Binomial(m, mu).logProbabilityOf(y)
  def llBinomial(
      y: DenseMatrix[Double],
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    val vals = m.toArray.zip(mu.toArray).zip(y.toArray).map {
      case((a, b), c) => Array(a, b, c)
    }
    val ll = vals.map {
      x => Array(Binomial(x(0).toInt, x(1)).logProbabilityOf(x(2).toInt))
    }.map(y => y(0))
    new DenseMatrix(rows = y.rows, cols = 1, ll)
  }
}
