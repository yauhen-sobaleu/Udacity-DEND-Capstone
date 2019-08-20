package com.dend.capstone.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.{ZonedDateTime, LocalDate}
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

    val rawDimTable = df
      .select(raw_col_names.map(name => col(name)):_*)
      .distinct                  /** apply SELECT DISTINCT logic, select only distinct values for creating a dim table */
      .coalesce(1) /** coalesce df in one partition to guarantee that monotonically_increasing_id() provides consecutive results */
      .orderBy(raw_col_names(0)) /** order by the first column*/
      .withColumn(dim_id, monotonically_increasing_id()) /** add unique and consecutive surrogate key*/

    val col_names = Seq(dim_id) ++ raw_col_names
    val dimTable = rawDimTable.select(col_names.map(name => col(name)):_*)

    dimTable
  }

  /** Creates a dataframe with dates dimension
    *
    * @param startDate  first date of a dimension
    * @param endDate    last date of a dimension
    * @param spark      sparkSession
    * @return
    */
  def createDateDimTable(startDate: String, endDate: String, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val df_dates = spark.sparkContext
      .parallelize(createDateSequence(startDate, endDate).map(_.toString).toSeq)
      .toDF("date")
      .withColumn("date", col("date").cast("date"))
      .coalesce(1)

    val date_dim = df_dates
      .withColumn("date_dim_id", monotonically_increasing_id() + 1)
      .withColumn("date_type", lit("date"))
      .withColumn("date_actual", col("date"))
      .withColumn("epoch", unix_timestamp(col("date").cast("timestamp")))
      .withColumn("day_name", date_format(col("date"), "EEEE"))
      .withColumn("day_of_week", dayofweek(col("date")))
      .withColumn("day_of_month", dayofmonth(col("date")))
      .withColumn("day_of_year", dayofyear(col("date")))
      .withColumn("week_of_month", date_format(col("date"), "W"))
      .withColumn("week_of_year", weekofyear(col("date")))
      .withColumn("month_actual", month(col("date")))
      .withColumn("month_name", date_format(col("date"), "MMMMM"))
      .withColumn("month_name_abbreviated", date_format(col("date"), "MMM"))
      .withColumn("quarter_actual", quarter(col("date")))
      .withColumn("quarter_name",
        when(col("quarter_actual") === 1, "First")
          when(col("quarter_actual") === 2, "Second")
          when(col("quarter_actual") === 3, "Third")
          when(col("quarter_actual") === 4, "Fourth"))
      .withColumn("year_actual", year(col("date")))
      .drop("date")

    /** create one record to cover use cases where a date is unknown (for safe null joins) */
    val max_id = date_dim.agg("date_dim_id" -> "max").first().getLong(0) + 1
    val unknownRecord = Seq((max_id, "unknown", null, null, null, null, null, null, null, null, null, null, null, null, null, null)).toDF()

    val finalDatesDF = date_dim.union(unknownRecord)

    finalDatesDF
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

  /** Creates an iterator with consecutive dates values
    *
    * @param startDate start date of an interval
    * @param endDate   end date of an interval
    * @return
    */
  def createDateSequence(startDate: String, endDate: String) : Iterator[LocalDate] = {
    def dayIterator(start: LocalDate, end: LocalDate) = Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)

    val dates = dayIterator(LocalDate.parse(startDate), LocalDate.parse(endDate))

    dates
  }

}
