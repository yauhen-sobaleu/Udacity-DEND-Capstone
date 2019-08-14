package com.dend.capstone

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._
import com.dend.capstone.utils.sparkUtils._

object Capstone {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DEND_Capstone_Project")
      .getOrCreate()

    import spark.implicits._

    val wildfiresDF = spark.read.format("jdbc")
      .option("url", "jdbc:sqlite:/Users/yauhensobaleu/Downloads/FPA_FOD_20170508.sqlite")
      .option("dbtable",
        """
          |(select
          |   OBJECTID,
          |   FOD_ID,
          |   FPA_ID,
          |   SOURCE_SYSTEM_TYPE,
          |   SOURCE_SYSTEM,
          |   NWCG_REPORTING_AGENCY,
          |   NWCG_REPORTING_UNIT_ID,
          |   NWCG_REPORTING_UNIT_NAME,
          |   SOURCE_REPORTING_UNIT,
          |   SOURCE_REPORTING_UNIT_NAME,
          |   LOCAL_FIRE_REPORT_ID,
          |   LOCAL_INCIDENT_ID,
          |   FIRE_CODE,
          |   FIRE_NAME,
          |   ICS_209_INCIDENT_NUMBER,
          |   ICS_209_NAME,
          |   MTBS_ID,
          |   MTBS_FIRE_NAME,
          |   COMPLEX_NAME,
          |   FIRE_YEAR,
          |   DISCOVERY_DATE,
          |   DISCOVERY_DOY,
          |   DISCOVERY_TIME,
          |   STAT_CAUSE_CODE,
          |   STAT_CAUSE_DESCR,
          |   CONT_DATE,
          |   CONT_DOY,
          |   CONT_TIME,
          |   FIRE_SIZE,
          |   FIRE_SIZE_CLASS,
          |   LATITUDE,
          |   LONGITUDE,
          |   OWNER_CODE,
          |   OWNER_DESCR,
          |   STATE,
          |   COUNTY,
          |   FIPS_CODE,
          |   FIPS_NAME
          |FROM usa_fires)""".stripMargin)
      .option("driver", "org.sqlite.JDBC")
      .load()

    def stageWildfiresDF(df: DataFrame): DataFrame = {

      /** prepare a list of Decimal columns*/
      val decimalColumns = for (dtype <- df.dtypes if (dtype._2.startsWith("DecimalType")))
        yield dtype._1

      /** convert integer-valued columns to IntegerType */
      val dfWithIntegers = df
        .withColumn("FOD_ID", $"FOD_ID".cast("int"))
        .withColumn("FIRE_YEAR", $"FIRE_YEAR".cast("int"))
        .withColumn("DISCOVERY_DOY", $"DISCOVERY_DOY".cast("int"))
        .withColumn("STAT_CAUSE_CODE", $"STAT_CAUSE_CODE".cast("int"))
        .withColumn("CONT_DOY", $"CONT_DOY".cast("int"))
        .withColumn("OWNER_CODE", $"OWNER_CODE".cast("int"))

      /** decrease precision values in Decimal cols */
      val properDecimalSize = "decimal(10,2)"
      val coordinatesDecimalSize = "decimal(11,8)"

      val dfWithIntegersProperDecimals = dfWithIntegers
        .withColumn("DISCOVERY_DATE", $"DISCOVERY_DATE".cast(properDecimalSize))
        .withColumn("CONT_DATE", $"CONT_DATE".cast(properDecimalSize))
        .withColumn("FIRE_SIZE", $"FIRE_SIZE".cast(properDecimalSize))
        .withColumn("LATITUDE", $"LATITUDE".cast(coordinatesDecimalSize))
        .withColumn("LONGITUDE", $"LONGITUDE".cast(coordinatesDecimalSize))

      /** combine separate date and time columns*/
      val dfWithTimestampsDiscovery = concatDateAndTime(dfWithIntegersProperDecimals, "DISCOVERY")
      val dfWithTimestampsCont = concatDateAndTime(dfWithTimestampsDiscovery, "CONT")

      /** lower case all column names and drop redundant data */
      val stagedDF = dfWithTimestampsCont
        .toDF(dfWithTimestampsCont.columns.map(name => name.toLowerCase()):_*)
        .drop(
          "fire_year",
          "disovery_doy",
          "discovery_time",
          "cont_doy",
          "cont_time"
        )
      stagedDF
    }

    val stagedDF = stageWildfiresDF(wildfiresDF)

    println(stagedDF.show(5))
    println(stagedDF.printSchema())
  }
}
