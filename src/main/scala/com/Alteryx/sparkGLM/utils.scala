package com.Alteryx.sparkGLM

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

object utils {

  def matchCols (estDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    val missingCols: Array[Column] = estDF.columns.diff(scoreDF.columns).map { field =>
      lit(0).cast(DoubleType).as(field)
    }
    scoreDF.select(missingCols :+ col("*"):_*)
  }

}
