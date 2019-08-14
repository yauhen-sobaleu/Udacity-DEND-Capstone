package com.dend.capstone.utils

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import java.time.{ZonedDateTime}
import java.time.format.DateTimeFormatter

/** Object with functions to simplify spark modelling transformations */
object sparkUtils {

  /** Converts dates in Julian format to modern representation*/
  def convertJulianDate = udf((julianDate: Double) => {
    val julianDayNumber = math.floor(julianDate).toLong
    val julianDayFraction = julianDate - julianDayNumber
    val julianDayFractionToNanoSeconds = math.floor(julianDayFraction * 24 * 60 * 60 * math.pow(10, 9)).toLong

    val bcEraDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:z:G")
    val julianDateStartDate = ZonedDateTime.parse("4714-11-24 12:00:00:GMT:BC", bcEraDateFormat)

    val zonedDateTime = julianDateStartDate.plusDays(julianDayNumber).plusNanos(julianDayFractionToNanoSeconds)

    zonedDateTime.toLocalDate.toString()
  }
  )

  /** Utility function for creating dimensions tables out of staged DataFrame
    *
    * @param df   any staged spark DataFrame
    * @param id   name of id column of a dimension being created
    * @param cols sequence of staged DataFrame's columns names to create a dimension
    * @return     spark DataFrame
    */
  def createDimTable(df: DataFrame, id: String, cols: Seq[String]): DataFrame = {
    val raw_col_names = cols.map(name => name.toLowerCase())
    val dim_id = id.toLowerCase()
    val nullPlaceholder = "unknown"

    val rawDimTable = df
      .select(raw_col_names.map(name => col(name)):_*)
      .na.fill(nullPlaceholder)  /** replace all nulls with a string placeholder for easier joins */
      .distinct                  /** apply SELECT DISTINCT logic, select only distinct values for creating a dim table */
      .coalesce(1) /** coalesce df in one partition to guarantee that monotonically_increasing_id() provides consecutive results */
      .orderBy(raw_col_names(0)) /** order by the first column*/
      .withColumn(dim_id, monotonically_increasing_id()) /** add unique and consecutive surrogate key*/

    val col_names = Seq(dim_id) ++ raw_col_names
    val dimTable = rawDimTable.select(col_names.map(name => col(name)):_*)

    dimTable
  }

  /** Concatenates separate date and time columns in USA wildfires dataset into one datetime column
    *
    * @param df       spark DataFrame
    * @param colName  prefix of a columns to concatenate
    * @return         spark DataFrame with additional datetime column
    */
  def concatDateAndTime(df:DataFrame, colName: String): DataFrame = {

    val dateColumn = colName + "_DATE"
    val timeColumn = colName + "_TIME"

    val resultDF = df
      .withColumn(colName + "_TIMESTAMP",
        concat(
          convertJulianDate(col(dateColumn)),
          lit(" "),
          regexp_replace(col(timeColumn), "..(?!$)", "$0:"),
          lit(":00")
          ).cast("timestamp"))

    resultDF
  }
}
