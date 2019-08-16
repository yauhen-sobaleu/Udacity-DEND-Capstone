package com.dend.capstone

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.log4j._

import com.dend.capstone.utils.sparkUtils._
import com.dend.capstone.utils.sparkModelling._


object Capstone {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

      Logger.getLogger("org").setLevel(Level.ERROR)

      val sc = SparkContext.getOrCreate()

      val conf = new SparkConf

      val spark = SparkSession
        .builder()
        .appName("DEND_Capstone_Project")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload","true")
        .config("spark.speculation", "false")
        .getOrCreate()

      /** import config values */
      val datalakeBucket = sc.getConf.get("spark.rootbucket")
      val wildfiresPath = sc.getConf.get("spark.wildfirespath")
      val weatherOutliersPath = sc.getConf.get("spark.weatherpath")
      val awsKey = sc.getConf.get("spark.awskey")
      val awsSecret = sc.getConf.get("spark.awssecret")

      sc.hadoopConfiguration.set("fs.s3a.access.key", awsKey)
      sc.hadoopConfiguration.set("fs.s3a.secret.key", awsSecret)
      sc.hadoopConfiguration.set("fs.s3a.attempts.maximum", "30")  /** Preventing AmazonHttpClient:448 - Unable to execute HTTP request error */

      import spark.implicits._

      /** Read in USA wildfires dataset */
      val wildfiresDF = spark.read.format("jdbc")
        .option("url", "jdbc:sqlite:" + wildfiresPath)
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
            |FROM Fires)""".stripMargin)
        .option("driver", "org.sqlite.JDBC")
        .load()

      /** Read in USA weather outliers dataset */
      val weatherOutliers = spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .load(weatherOutliersPath)
        .drop("id", "station_name")


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
            "discovery_doy",
            "discovery_time",
            "cont_doy",
            "cont_time"
          )
        stagedDF
      }

      def stageWeatherDF(df: DataFrame): DataFrame = {

        /** We're interested in temperature data starting from 1992-01-01 only */
        val stagedWeatherDF = df
          .withColumn("date_str", $"date_str".cast("date"))
          .filter($"date_str" >= "1992-01-01")

        stagedWeatherDF
      }

      /** Stage dataframes for further dimensional modelling */
      val stagedWildfiresDF = stageWildfiresDF(wildfiresDF)
      val stagedWeatherDF = stageWeatherDF(weatherOutliers)


      /** === Create dimensions from wildfires dataset === */

      val sources = createDimTable(stagedWildfiresDF, sourcesDimId, sourcesCols)
      val reports = createDimTable(stagedWildfiresDF, reportsDimId, reportsCols)
      val fireNames = createDimTable(stagedWildfiresDF, fireNamesDimId, fireNamesCols)
      val fireCauses = createDimTable(stagedWildfiresDF, fireCausesDimId, fireCausesCols)
      val fireSizes = createDimTable(stagedWildfiresDF, fireSizesDimId, fireSizesCols)
            .withColumn("lower_bound",
              when($"fire_size_class" === "A", 0)
                when($"fire_size_class" === "B", 0.26)
                when($"fire_size_class" === "C", 10.0)
                when($"fire_size_class" === "D", 100)
                when($"fire_size_class" === "E", 300)
                when($"fire_size_class" === "F", 1000)
                when($"fire_size_class" === "G", 5000)
            )
            .withColumn("upper_bound",
              when($"fire_size_class" === "A", 0.25)
                when($"fire_size_class" === "B", 9.9)
                when($"fire_size_class" === "C", 99.9)
                when($"fire_size_class" === "D", 299)
                when($"fire_size_class" === "E", 999)
                when($"fire_size_class" === "F", 4999)
                when($"fire_size_class" === "G", null)
            )
      val owners = createDimTable(stagedWildfiresDF, ownersDimId, ownersCols)
      val locations = createDimTable(stagedWildfiresDF, locationsDimId, locationsCols)
      val weatherTypes = createDimTable(stagedWeatherDF, weatherTypesDimId, weatherTypesCols)

      val df_dates = spark.sparkContext
        .parallelize(createDateSequence("1990-01-01", "2020-01-01").map(_.toString).toSeq)
        .toDF("date")
        .withColumn("date", col("date").cast("date"))
        .coalesce(1)

      val dates = createDateDimTable(df_dates)

      /** === Create FACT table for wildfires dataset === */

      val disc_dates = dates.toDF(dates.columns.map(name => "disc_" + name):_*)
      val cont_dates = dates.toDF(dates.columns.map(name => "cont_" + name):_*)

      val firesFact = stagedWildfiresDF
        .withColumn("disc_date_actual", $"discovery_timestamp".cast("date"))
        .withColumn("cont_date_actual", $"cont_timestamp".cast("date"))
        .join(broadcast(disc_dates), Seq("disc_date_actual"), "inner")
        .join(broadcast(cont_dates), Seq("cont_date_actual"), "inner")
        .join(broadcast(sources), sourcesCols, "inner")
        .join(broadcast(reports), reportsCols, "inner")
        .join(fireNames, fireNamesCols, "inner")
        .join(broadcast(fireCauses), fireCausesCols, "inner")
        .join(broadcast(fireSizes), fireSizesCols, "inner")
        .join(broadcast(owners), ownersCols, "inner")
        .join(broadcast(locations), locationsCols, "inner")
        .select(
          $"objectid" as "fire_id",
          $"fpa_id",
          $"disc_date_dim_id" as "discovery_date_id",
          $"cont_date_dim_id" as "cont_date_dim_id",
          $"source_id",
          $"reporter_id",
          $"fire_name_id",
          $"fire_size_id",
          $"fire_cause_id",
          $"owner_id",
          $"location_id",
          $"fire_size",
          $"latitude",
          $"longitude",
          $"discovery_timestamp",
          $"cont_timestamp",
          year($"discovery_timestamp").alias("year")

        )

      /** === Create FACT table for weather outliers dataset === */

      val weather_dates = dates.toDF(dates.columns.map(name => "weather_" + name):_*)

      val weatherOutliersFact = stagedWeatherDF
        .withColumn("weather_date_actual", $"date_str")
        .join(broadcast(weather_dates), Seq("weather_date_actual"), "inner")
        .join(broadcast(weatherTypes), Seq("type"), "inner")
        .select(
          $"serialid" as "weather_outlier_id",
          $"weather_date_dim_id",
          $"weather_type_id",
          $"longitude",
          $"latitude",
          $"max_temp",
          $"min_temp",
          year($"date_str").alias("year")
        )

      /** === Persist modelled data on AWS S3 === */

      /** persist dimensions */
      sources.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + sourcesBucket)
      reports.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + reportsBucket)
      fireNames.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + fireNamesBucket)
      fireCauses.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + fireCausesBucket)
      fireSizes.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + fireSizesBucket)
      owners.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + ownersBucket)
      locations.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + locationsBucket)
      weatherTypes.write.mode("overwrite").parquet("s3a://" + datalakeBucket + "/" + weatherTypesBucket)

      /** persist facts */
      firesFact.coalesce(10).write.partitionBy("year").parquet("s3a://" + datalakeBucket + "/" + wildfiresFactBucket)
      weatherOutliersFact.coalesce(10).write.partitionBy("year").parquet("s3a://" + datalakeBucket + "/" + weatherOutliersFactBucket)

      println("Done")

      sc.stop()

  }

}
