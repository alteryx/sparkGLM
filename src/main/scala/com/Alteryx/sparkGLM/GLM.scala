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
  ///// Binomial variance
  def varianceBinomial(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    mu :* (1.0 :+ (-1.0 :* (mu :/ m)))
  }

  ///// Binomial likelihood
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

  //// Binomial deviance
  def devBinomial(
      y: DenseMatrix[Double],
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): Double = {
    val my = m :+ (-1.0 :* mu)
    val rowValue = y :* breeze.numerics.log(max(y, 1.0) :/ mu) + my :* breeze.numerics.log(max(my, 1.0) :/ (m :+ (-1.0 :* mu)))
    val deviance = (utils.repValue(1.0, rowValue.rows) * rowValue).toArray
    2*deviance(0)
  }

  ///// Binomial Deviance Residuals (included for possible future use)
//  def devResidsBinomial(
//      y: DenseMatrix[Double],
//      mu: DenseMatrix[Double],
//      m: DenseMatrix[Double]): DenseMatrix[Double] = {
//    val vals = y.toArray.zip(mu.toArray).zip(m.toArray).map {
//      case((a, b), c) => Array(a, b, c)
//    }
//    val devs = vals.map {
//      x => Array(utils.sign(x(0) - x(1)) * sqrt(2.0 * ))
//    }.map(y => y(0))
//      new DenseMatrix(rows = y.rows, cols = 1, devs)
//    new DenseMatrix(rows = y.rows, cols = 1, devs)
//  }

  /// Link function specific methods
  //// Binomial
  ///// Logit
  def linkLogit(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    breeze.numerics.log(mu :/ (m :+ (-1.0 :* mu)))
  }
  def lPrimeLogit(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    m :/ (mu :* (m :+ (-1.0 :* mu)))
  }
  def unlinkLogit(
      eta: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    m :/ (1.0 :+ breeze.numerics.exp(-eta))
  }

  ///// Probit
  def linkProbit(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    val muM = mu.toArray.zip(m.toArray)
    val linkRow = muM.map {
      x => Array(Gaussian(0.0, 1.0).icdf(x._1/x._2))
    }.map(y => y(0))
    new DenseMatrix(rows = mu.rows, cols = 1, linkRow)
  }
  def lPrimeProbit(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    val link = linkProbit(mu, m)
    val mLink = m.toArray.zip(link.toArray)
    val lPrimeRow = mLink.map {
      x => Array(1.0 / (x._1 * Gaussian(0.0, 1.0).pdf(x._2)))
    }.map(y => y(0))
    new DenseMatrix(rows = mu.rows, cols = 1, lPrimeRow)
  }
  def unlinkProbit(
      eta: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    val mEta = m.toArray.zip(eta.toArray)
    val unlinkRow = mEta.map {
      x => Array(x._1 * Gaussian(0.0, 1.0).cdf(x._2))
    }.map(y => y(0))
    new DenseMatrix(rows = eta.rows, cols = 1, unlinkRow)
  }


  ///// Complimentary log-log
  def linkCloglog(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    breeze.numerics.log(-1.0 :* breeze.numerics.log(1.0 :+ -1.0 :* (mu :/ m)))
  }
  def lPrimeCloglog(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    1.0 :/ ((mu :+ (-1.0 :* m)) :* breeze.numerics.log(1.0 :+ (-1.0 :* (mu :/ m))))
  }
  def unlinkCloglog(
      y: DenseMatrix[Double],
      eta: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    m :* (1.0 :+ -1.0 :* breeze.numerics.exp(-breeze.numerics.exp(eta)))
  }
}
