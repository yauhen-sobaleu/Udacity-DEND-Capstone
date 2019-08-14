package com.dend.capstone.utils

object sparkModelling {

  val sourceCols = Seq(
    "source_system_type",
    "source_system",
    "source_reporting_unit",
    "source_reporting_unit_name")
  val sourceDimId = "source_id"

  val reportsCols = Seq(
    "nwcg_reporting_agency",
    "nwcg_reporting_unit_id",
    "nwcg_reporting_unit_name")
  val reportsDimId = "reporter_id"

  val fireNamesCols = Seq(
    "fire_name",
    "ics_209_name",
    "fire_code",
    "mtbs_fire_name")
  val fireNamesDimId = "fire_name_id"

  val fireCausesCols = Seq(
    "stat_cause_code",
    "stat_cause_descr")
  val fireCausesDimId = "fire_cause_id"

  val fireSizesCols = Seq(
    "fire_size_class")
  val fireSizesDimId = "fire_size_id"

  val ownersCols = Seq(
    "owner_code",
    "owner_descr")
  val ownersDimId = "owner_id"

  val locationsCols = Seq(
    "state",
    "fips_name")
  val locationsDimId = "location_id"

  val weatherTypesCols = Seq(
    "type")
  val weatherTypesDimId = "weather_type_id"
}
