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


case class PreGLM(coefs: DenseMatrix[Double],
                  stdErr: Array[Double],
                  deviance: Double,
                  nullDeviance: Double,
                  pearson: Double,
                  loglik: Double,
                  iter: Int,
                  nrow: Double,
                  npart: Int)

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
              iter: Int,
              nrow: Double,
              npart: Int)

case class ZWobj(z: RowPartitionedMatrix, w: RowPartitionedMatrix)

object GLM {

  // Summary methods that are minimally related to the choice of family
  /// A method to create a GLM object form a preGLM object and other information
  def createObj(
                x: DataFrame,
                y: DataFrame,
                pre: PreGLM,
                family: String,
                link: String): GLM = {
    val nrow = y.count.toDouble
    val se = pre.stdErr
    val dfResidual = nrow - se.size.toDouble
    val dfNull = nrow - 1.0
    val pDispersion = pre.pearson / dfResidual
    val aic = -2.0 * pre.loglik + 2.0 * se.size.toDouble
    new GLM(x.columns,
        y.columns(0),
        pre.coefs,
        se,
        dfResidual,
        dfNull,
        pre.deviance,
        pre.nullDeviance,
        pDispersion,
        pre.pearson,
        pre.loglik,
        family,
        link,
        aic,
        pre.iter,
        pre.nrow,
        pre.npart)
  }
  /// A method to calculate the pre-Pearson chi-square values
  def pearsonCalc(
      y: DenseMatrix[Double],
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double],
      family: String): DenseMatrix[Double] = {
    val variance = if (family == "binomial") {
        varianceBinomial(mu, m)
      }else{
        varianceBinomial(mu, m)
      }
    pow((y :+ (-1.0 :* mu)), 2.0) :/ variance
  }

  /// Extending the Pearson calculation to partioned data
  def pearsonCalcMultiple(
      ym: RowPartitionedMatrix,
      mu: RowPartitionedMatrix,
      m: RowPartitionedMatrix,
      family: String): Double = {
    val ymum = ym.rdd.zip(mu.rdd).zip(m.rdd).map {
      case((a, b), c) => (a.mat, b.mat, c.mat)
    }
    val pearsonRows1 = ymum.map { part =>
      pearsonCalc(part._1, part._2, part._3, family = family)
    }
    val pearsonRows = RowPartitionedMatrix.fromMatrix(pearsonRows1)
    val pearsonSums = pearsonRows.rdd.map( x => sum(x.mat(::, 0)))
    pearsonSums.collect.reduce(_+_)
  }


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

  /// Extending the binomial ll calculation to partioned data
  def llBinomialMultiple(
      ym: RowPartitionedMatrix,
      mu: RowPartitionedMatrix,
      m: RowPartitionedMatrix): Double = {
    val ymum = ym.rdd.zip(mu.rdd).zip(m.rdd).map {
      case((a, b), c) => (a.mat, b.mat, c.mat)
    }
    val ll1 = ymum.map { part =>
      llBinomial(part._1, part._2, part._3)
    }
    val ll = RowPartitionedMatrix.fromMatrix(ll1)
    val llSums = ll.rdd.map( x => sum(x.mat(::, 0)))
    llSums.collect.reduce(_+_)
  }

  //// Binomial deviance
  def devBinomial(
      y: DenseMatrix[Double],
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): Double = {
    val my = m :+ (-1.0 :* y)
    val rowValue = (y :* breeze.numerics.log(max(y, 1.0) :/ mu)) :+ (my :* breeze.numerics.log(max(my, 1.0) :/ (m :+ (-1.0 :* mu))))
    val deviance = (utils.repValue(1.0, rowValue.rows).t * rowValue).toArray
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
    m :/ (1.0 :+ breeze.numerics.exp(-1.0 :* eta))
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
    breeze.numerics.log(-1.0 :* breeze.numerics.log(1.0 :+ (-1.0 :* (mu :/ m))))
  }
  def lPrimeCloglog(
      mu: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    1.0 :/ ((mu :+ (-1.0 :* m)) :* breeze.numerics.log(1.0 :+ (-1.0 :* (mu :/ m))))
  }
  def unlinkCloglog(
      eta: DenseMatrix[Double],
      m: DenseMatrix[Double]): DenseMatrix[Double] = {
    m :* (1.0 :+ (-1.0 :* breeze.numerics.exp(-breeze.numerics.exp(eta))))
  }

  ///// Fit for the binomial family
  def fitSingleBinomial(
      ym: DenseMatrix[Double],
      xm: DenseMatrix[Double],
      link: String,
      tol: Double = 1e-6,
      offset: DenseMatrix[Double],
      m: DenseMatrix[Double],
      verbose: Boolean = false): PreGLM = {
    // Initialize values
    var mu = utils.repValue(sum(ym(::, 0))/ym.rows.toDouble, ym.rows)
    var eta = if(link == "logit"){
        linkLogit(mu, m)
      }else if(link == "probit"){
        linkProbit(mu, m)
      }else{
        linkCloglog(mu, m)
      }
    var dev = devBinomial(ym, mu, m)
    val nullDev = dev
    var devOld = dev
    var deltad = 1.0
    var iter = 0
    var w = utils.repValue(1.0, ym.rows)
    var z = w
    var grad = w
    var mod = new utils.WLSObj(utils.repValue(0.0, 2), DenseVector(0.0, 0.0))
    // The IRLS iterations
    while(scala.math.abs(deltad) > tol){
      grad = if(link == "logit"){
          lPrimeLogit(mu, m)
        }else if(link == "probit"){
          lPrimeProbit(mu, m)
        }else{
          lPrimeCloglog(mu, m)
        }
      w = 1.0 :/ (varianceBinomial(mu, m) :* pow(grad, 2))
      z = eta :+ ((ym :+ (-1.0 :* mu)) :* grad) :+ (-1.0 :* offset)
      mod = utils.wlsSingle(xm, z, w)
      eta = (xm * mod.coefs) :+ offset
      mu = if(link == "logit"){
          unlinkLogit(eta, m)
        }else if(link == "probit"){
          unlinkProbit(eta, m)
        }else{
          unlinkCloglog(eta, m)
        }
      devOld = dev
      dev = devBinomial(ym, mu, m)
      deltad = dev - devOld
      iter = iter + 1
      if(verbose) println(iter.toString + "\t" + deltad.toString)
    }
    // Calculate the model summary statistics
    val stdError = mod.diagDesign.toArray
    val pearsonRow = pearsonCalc(ym, mu, m, "binomial")
    val pearson = sum(pearsonRow(::, 0))
    val llRow = llBinomial(ym, mu, m)
    val ll = sum(llRow(::, 0))
    val nrow = xm.rows.toDouble
    val npart = 1
    new PreGLM(mod.coefs, stdError, dev, nullDev, pearson, ll, iter, nrow, npart)
  }

  // A method that calutes eta (the linear predictor) for partiioned data.
  // This is used within IRLS iterations, the initial value of eta is
  // calculated directly from the data.
  // This will likely be the start of a predict method for partioned data.
  def etaCreate(
      xm: RowPartitionedMatrix,
      offset: RowPartitionedMatrix,
      coefs: DenseMatrix[Double]): RowPartitionedMatrix = {
    val xmOffset = xm.rdd.zip(offset.rdd).map {
      case(a, b) => (a.mat, b.mat)
    }
    val eta = xmOffset.map { part =>
      (part._1 * coefs) :+ part._2
    }
    RowPartitionedMatrix.fromMatrix(eta)
  }

  def muCreate(
      eta: RowPartitionedMatrix,
      m: RowPartitionedMatrix,
      link: String): RowPartitionedMatrix = {
    val etaM = eta.rdd.zip(m.rdd).map {
      case(a, b) => (a.mat, b.mat)
    }
    val mu = if(link == "logit") {
      etaM.map { part =>
        unlinkLogit(part._1, part._2)
      }
    }else if(link == "probit") {
      etaM.map { part =>
        unlinkProbit(part._1, part._2)
      }
    }else{
      etaM.map { part =>
        unlinkCloglog(part._1, part._2)
      }
    }
    RowPartitionedMatrix.fromMatrix(mu)
  }

  // A method that calculates the new weights and z values for the binomial
  // family.
  def zwCreateBinomial(
      ym: RowPartitionedMatrix,
      m: RowPartitionedMatrix,
      eta: RowPartitionedMatrix,
      offset: RowPartitionedMatrix,
      link: String): ZWobj = {
    val yme = ym.rdd.zip(m.rdd).zip(eta.rdd).map {
      case((a, b), c) => (a.mat, b.mat, c.mat)
    }
    val theObj = if(link == "logit") {
      yme.map { part =>
        (part._3 :+ ((part._1 :+ (-1.0 :* unlinkLogit(part._3, part._2))) :* lPrimeLogit(part._3, part._2)));
        (1.0 :/ (varianceBinomial(unlinkLogit(part._3, part._2), part._2) :* pow(lPrimeLogit(part._3, part._2), 2)))
      }
    }else if(link == "probit") {
      yme.map { part =>
        (part._3 :+ ((part._1 :+ (-1.0 :* unlinkProbit(part._3, part._2))) :* lPrimeProbit(part._3, part._2)));
        (1.0 :/ (varianceBinomial(unlinkProbit(part._3, part._2), part._2) :* pow(lPrimeProbit(part._3, part._2), 2)))
      }
    }else{
      yme.map { part =>
        (part._3 :+ ((part._1 :+ (-1.0 :* unlinkCloglog(part._3, part._2))) :* lPrimeCloglog(part._3, part._2)));
        (1.0 :/ (varianceBinomial(unlinkCloglog(part._3, part._2), part._2) :* pow(lPrimeCloglog(part._3, part._2), 2)))
      }
    }
    val z0 = RowPartitionedMatrix.fromMatrix(theObj.map(x => x(::, 0).toDenseMatrix))
    val zOffset = z0.rdd.zip(offset.rdd).map {
      case(a, b) => (a.mat, b.mat)
    }
    val z1 = zOffset.map { part =>
      part._1 :+ (-1.0 :* part._2)
    }
    val w = theObj.map( x => x(::, 1).toDenseMatrix)
    new ZWobj(RowPartitionedMatrix.fromMatrix(z1), RowPartitionedMatrix.fromMatrix(w))
  }

  def createBinomialDeviance(
      ym: RowPartitionedMatrix,
      mu: RowPartitionedMatrix,
      m: RowPartitionedMatrix): Double = {
    val ymum = ym.rdd.zip(mu.rdd).zip(m.rdd).map {
      case((a, b), c) => (a.mat, b.mat, c.mat)
    }
    val partDev = ymum.map { part =>
      devBinomial(part._1, part._2, part._3)
    }
    partDev.collect.reduce(_+_)
  }

  def fitMultipleBinomial(
      ym: RowPartitionedMatrix,
      xm: RowPartitionedMatrix,
      uno: RowPartitionedMatrix,
      link: String,
      tol: Double = 1e-6,
      offset: RowPartitionedMatrix,
      m: RowPartitionedMatrix,
      verbose: Boolean = false): PreGLM = {
    // Initialize values
    val ySums = ym.rdd.map(y => sum(y.mat(::, 0)))
    val nrow = ym.getDim._1
    val npart = xm.rdd.partitions.size
    val yMean = (ySums.collect.reduce(_+_))/nrow.toDouble
    val mu1 = uno.rdd.map {part => yMean :* part.mat}
    var mu = RowPartitionedMatrix.fromMatrix(mu1)
    val muM = mu.rdd.zip(m.rdd).map {
      case(a, b) => (a.mat, b.mat)
    }
    var eta1 = if(link == "logit"){
        muM.map { part =>
          linkLogit(part._1, part._2)
        }
      }else if(link == "probit"){
        muM.map { part =>
          linkProbit(part._1, part._2)
        }
      }else{
        muM.map { part =>
          linkCloglog(part._1, part._2)
        }
      }
    var eta = RowPartitionedMatrix.fromMatrix(eta1)
    var dev = createBinomialDeviance(ym, mu, m)
    val nullDev = dev
    var devOld = dev
    var deltad = 1.0
    var iter = 0
    var zw = new ZWobj(z = uno, w = uno)
    var grad = uno
    var mod = new utils.WLSObj(utils.repValue(0.0, 2), DenseVector(0.0, 0.0))
    // The IRLS iterations
    while(scala.math.abs(deltad) > tol){
      zw = zwCreateBinomial(ym, m, eta, offset, link)
      mod = utils.wlsMultiple(xm, zw.z, zw.w)
      eta = etaCreate(xm, offset, mod.coefs)
      mu = muCreate(eta, m, link)
      devOld = dev
      dev = createBinomialDeviance(ym, mu, m)
      deltad = dev - devOld
      iter = iter + 1
      if(verbose) println(iter.toString + "\t" + deltad.toString)
    }
    // Calculate the model summary statistics
    val stdError = mod.diagDesign.toArray
    val pearson = pearsonCalcMultiple(ym, mu, m, "binomial")
//    val pearson = sum(pearsonRow(::, 0))
//    val llRow = llBinomial(ym, mu, m)
    val ll = llBinomialMultiple(ym, mu, m)
    new PreGLM(mod.coefs, stdError, dev, nullDev, pearson, ll, iter, nrow, npart)
  }

// The fit methods for the case of a single data partition
/// The case of no provided offset or group sizes
  def fitSingle(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      tol: Double,
      verbose: Boolean): PreGLM = {
    // Convert the DataFrames to DenseMatrix objects
    val xm = utils.dfToDenseMatrix(x)
    val ym = utils.dfToDenseMatrix(y)
    // The default group sizes
    val m = utils.repValue(1.0, ym.rows)
    // The default offsets
    val offset = utils.repValue(0.0, ym.rows)
    val components = if (family == "Binomial") {
        fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
      }else{
        fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
      }
    components
  }
  /// The case of no provided offset
    def fitSingle(
        y: DataFrame,
        x: DataFrame,
        family: String,
        link: String,
        tol: Double,
        mDF: DataFrame,
        verbose: Boolean): PreGLM = {
      // Convert the DataFrames to DenseMatrix objects
      val xm = utils.dfToDenseMatrix(x)
      val ym = utils.dfToDenseMatrix(y)
      val m = utils.dfToDenseMatrix(mDF)
      // The default offsets
      val offset = utils.repValue(0.0, ym.rows)
      val components = if (family == "Binomial") {
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }else{
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }
      components
    }
    /// The case of no provided groups
    def fitSingle(
        y: DataFrame,
        x: DataFrame,
        offsetDF: DataFrame,
        family: String,
        link: String,
        tol: Double,
        verbose: Boolean): PreGLM = {
      // Convert the DataFrames to DenseMatrix objects
      val xm = utils.dfToDenseMatrix(x)
      val ym = utils.dfToDenseMatrix(y)
      val offset = utils.dfToDenseMatrix(offsetDF)
      // The default group sizes
      val m = utils.repValue(1.0, ym.rows)
      val components = if (family == "Binomial") {
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }else{
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }
      components
    }
/// The case of all arguments provided
    def fitSingle(
        y: DataFrame,
        x: DataFrame,
        offsetDF: DataFrame,
        family: String,
        link: String,
        tol: Double,
        mDF: DataFrame,
        verbose: Boolean): PreGLM = {
      // Convert the DataFrames to DenseMatrix objects
      val xm = utils.dfToDenseMatrix(x)
      val ym = utils.dfToDenseMatrix(y)
      val offset = utils.dfToDenseMatrix(offsetDF)
      val m = utils.dfToDenseMatrix(mDF)
      val components = if (family == "Binomial") {
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }else{
          fitSingleBinomial(ym, xm, link, tol, offset, m, verbose)
        }
      components
    }


  // The main fit methods
  /// No offset, group size, tolerence, or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No group, tolerence, or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset, tolerence, or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      m: DataFrame): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No tolerence or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      m: DataFrame): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset, group size, or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      tol: Double): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No group or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      tol: Double): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset or verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      tol: Double,
      m: DataFrame): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No verbose provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      tol: Double,
      m: DataFrame): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val verbose = false
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset, group size, or tolerence provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No group or tolerence provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset or tolerence provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      m: DataFrame,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No tolerence provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      m: DataFrame,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val tol = 1e-6
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset or group size provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      tol: Double,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No group provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      tol: Double,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// No offset provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      family: String,
      link: String,
      tol: Double,
      m: DataFrame,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }
  /// Everything provided
  def fit(
      y: DataFrame,
      x: DataFrame,
      offset: DataFrame,
      family: String,
      link: String,
      tol: Double,
      m: DataFrame,
      verbose: Boolean): GLM = {
    require(x.dtypes.forall(_._2 == "DoubleType"),
      "The provided DataFrame must contain all 'DoubleType' columns")
    require(x.rdd.partitions.size == y.rdd.partitions.size,
      "The two DataFrames must have the same number of paritions")
    require(x.count == y.count,
      "The two DataFrames must have the same number of rows")
    require(y.columns.size == 1,
      "The 'y' DataFrame must have only one column")
    val npart = x.rdd.partitions.size
    val components = if (npart == 1) {
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }else{ // Will change to fitDouble
        fitSingle(y, x, offset, family, link, tol, m, verbose)
      }
    createObj(x, y, components, family, link)
  }

  // A summary method.
  def summary(obj: GLM) = {
    val dfNDev = obj.nrow.toInt - 1
    val dfDev = obj.nrow.toInt - obj.xnames.size
    val coefArray = obj.coefs.toArray
    val zVals = coefArray.zip(obj.stdErr).map(x => x._1/x._2)
    val pVals = zVals.map(x => 2.0*(1.0 - Gaussian(0.0, 1.0).cdf(scala.math.abs(x))))
    var formula = obj.xnames(0)
    for (i <- 1 to (obj.xnames.size - 1)) {
      formula = formula + " + " + obj.xnames(i)
    }
    formula = obj.yname + " ~ " + formula
    println("Model:")
    println(formula)
    println("Family: " + obj.family)
    println("Link: " + obj.link)
    println("\n")
    println("Coefficients:")
    println(String.format("%-12s %12s %12s %12s %12s", "", "Estimate", "Std. Error", "z value", "Pr(>|z|)"))
    for (i <- 0 to (obj.xnames.size - 1)) {
      println(String.format("%-12s %12s %12s %12s %12s",obj.xnames(i), utils.sigDigits(coefArray(i), 6).toString, utils.sigDigits(obj.stdErr(i), 6).toString, utils.sigDigits(zVals(i), 6).toString, utils.sigDigits(pVals(i), 6).toString))
    }
    println("\n")
    println("Null deviance: " + utils.sigDigits(obj.nullDeviance, 6).toString + " on " + dfNDev.toString + " degress of freedom")
    println("Residual deviance: " + utils.sigDigits(obj.deviance, 6).toString + " on " + dfDev.toString + " degress of freedom")
    println("AIC: " + utils.sigDigits(obj.aic, 5).toString)
    println("\n")
    println("Number of Fisher Scoring iterations: " + obj.iter)
  }
}
