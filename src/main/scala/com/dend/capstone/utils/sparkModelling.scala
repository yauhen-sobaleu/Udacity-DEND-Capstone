package com.dend.capstone.utils

object sparkModelling {

  val sourcesCols = Seq(
    "source_system_type",
    "source_system",
    "source_reporting_unit",
    "source_reporting_unit_name")
  val sourcesDimId = "source_id"
  val sourcesBucket = "sources"

  val reportsCols = Seq(
    "nwcg_reporting_agency",
    "nwcg_reporting_unit_id",
    "nwcg_reporting_unit_name")
  val reportsDimId = "reporter_id"
  val reportsBucket = "reporters"

  val fireNamesCols = Seq(
    "fire_name",
    "ics_209_name",
    "fire_code",
    "mtbs_fire_name")
  val fireNamesDimId = "fire_name_id"
  val fireNamesBucket = "fire_names"

  val fireCausesCols = Seq(
    "stat_cause_code",
    "stat_cause_descr")
  val fireCausesDimId = "fire_cause_id"
  val fireCausesBucket = "fire_causes"

  val fireSizesCols = Seq(
    "fire_size_class")
  val fireSizesDimId = "fire_size_id"
  val fireSizesBucket = "fire_sizes"

  val ownersCols = Seq(
    "owner_code",
    "owner_descr")
  val ownersDimId = "owner_id"
  val ownersBucket = "owners"

  val locationsCols = Seq(
    "state",
    "fips_name")
  val locationsDimId = "location_id"
  val locationsBucket = "locations"

  val weatherTypesCols = Seq(
    "type")
  val weatherTypesDimId = "weather_type_id"
  val weatherTypesBucket = "weather_types"

  val wildfiresFactBucket = "fires_fact"
  val wildfiresFactPartitionCols = Seq("year", "month", "day")

  val weatherOutliersFactBucket = "weather_fact"
}
