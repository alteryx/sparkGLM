package com.Alteryx.sparkGLM

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object modelMatrix {
  def apply(df: DataFrame): DataFrame = {
    modelMatrix(df)
  }

  //modelMatrix takes an standard Spark DataFrame intended for use in a sparkGLM modeling
  //function, transforms each categorical variable into a set of k-1 binary columns, and
  //converts all columns to DoubleType. This allows for an easy conversion from DataFrame
  //to RowPartitionedMatrix while maintaining the ability to manipulate the data via
  //standard DataFrame methods.
  def modelMatrix (df: DataFrame): DataFrame = {
    var catVars = ArrayBuffer[Column]()
    var otherVars = ArrayBuffer[Column]()
    popVarArrays(df, catVars, otherVars)

    if (catVars.isEmpty) {
      castAll(df.select(otherVars:_*), DoubleType)
    } else {
      val varLevels = createLevelsMap(df, catVars)
      val newFields = otherVars ++ createDummies(varLevels)
      castAll(df.select(newFields:_*), DoubleType)
    }
  }

  //Separate string and non-string columns and store them in separate Array[Column]
  def popVarArrays(df: DataFrame,
                   categorical: ArrayBuffer[Column],
                   other: ArrayBuffer[Column]): Unit = {
                     df.dtypes.foreach{ elem =>
                       if (elem._2 == "StringType") {
                         categorical += df.apply(elem._1)
                       } else {
                         other += df.apply(elem._1)
                       }
                     }
                   }

  //Map each column to an array of its unique values
  def createLevelsMap(df: DataFrame,
                      fields: ArrayBuffer[Column]): Map[Column, Array[String]] = {
                        var levels: Map[Column, Array[String]] = Map()
                        fields.foreach { field =>
                          levels += (field -> getLevels(df, field))
                        }
                        return levels
                      }
  
  //Create an array of k-1 distinct categories for a single Column
  def getLevels(df: DataFrame, field: Column): Array[String] = {
    df.select(field).distinct.map(_.getString(0)).collect.sorted.slice(1, Integer.MAX_VALUE)
  }
  
  //Build an Array[Column] containing all of the new binary fields
  def createDummies(levelsMap: Map[Column, Array[String]]): Array[Column] = {
    levelsMap.keys.map { key =>
      explodeField(key, levelsMap(key))
    }.reduce { (acc, elem) =>
      acc ++ elem
    }
  }

  //Take a single Column and create an Array[Column] containing a binary field
  //for each distinct value
  def explodeField(field: Column, levels: Array[String]): Array[Column] = {
    levels.map { level =>
      when(field === level, 1).otherwise(0).as(f"$field%s_$level%s")
    }
  }


  //Cast all the fields of a DataFrame to a given DataType
  def castAll(df: DataFrame, to: DataType): DataFrame = {
    df.select( {
      df.columns.map { column =>
        df.apply(column).cast(to).as(column)
      }
    }:_* )
  }
}