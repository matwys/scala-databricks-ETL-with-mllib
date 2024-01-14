// Databricks notebook source
val cruiseShipDataPath = "/mnt/cruise-ship/bronze/cruise_ship_info.tsv"

// COMMAND ----------

val cruiseShipData = spark.read
    .option("delimiter", "\t").option("header", "true").option("inferSchema", "true").csv(cruiseShipDataPath)

// COMMAND ----------

display(cruiseShipData)

// COMMAND ----------

cruiseShipData.dtypes

// COMMAND ----------

def changeDecimalSeparator: (String => String) = { s => s.replace(',', '.') }
val changeDecimalSeparatorUDF = udf(changeDecimalSeparator)

// COMMAND ----------

val processedCruiseShipData = cruiseShipData.select(
  cruiseShipData("Ship_name"),
  cruiseShipData("Cruise_line"),
  cruiseShipData("Age"), 
  changeDecimalSeparatorUDF(cruiseShipData("Tonnage")).cast("double") as "Tonnage",
  changeDecimalSeparatorUDF(cruiseShipData("passengers")).cast("double") as "passengers",
  changeDecimalSeparatorUDF(cruiseShipData("length")).cast("double") as "length",
  changeDecimalSeparatorUDF(cruiseShipData("cabins")).cast("double") as "cabins",
  changeDecimalSeparatorUDF(cruiseShipData("passenger_density")).cast("double") as "passenger_density",
  changeDecimalSeparatorUDF(cruiseShipData("crew")).cast("double") as "crew"
  )

// COMMAND ----------

processedCruiseShipData.dtypes

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._

val deltaTable = DeltaTable.forPath(spark, "/mnt/cruise-ship/silver/cruise_ship")

deltaTable
  .as("tgt")
  .merge(
    processedCruiseShipData.as("src"),
    "src.Ship_name = tgt.Ship_name")
  .whenMatched
  .updateExpr(
    Map(
      "Ship_name" -> "src.Ship_name",
      "Cruise_line" -> "src.Cruise_line",
      "Age" -> "src.Age",
      "Tonnage" -> "src.Tonnage",
      "passengers" -> "src.passengers",
      "length" -> "src.length",
      "cabins" -> "src.cabins",
      "passenger_density" -> "src.passenger_density",
      "crew" -> "src.crew"
    ))
  .whenNotMatched
  .insertExpr(
    Map(
      "Ship_name" -> "src.Ship_name",
      "Cruise_line" -> "src.Cruise_line",
      "Age" -> "src.Age",
      "Tonnage" -> "src.Tonnage",
      "passengers" -> "src.passengers",
      "length" -> "src.length",
      "cabins" -> "src.cabins",
      "passenger_density" -> "src.passenger_density",
      "crew" -> "src.crew"
    ))
  .execute()

// COMMAND ----------

dbutils.notebook.exit("Bronze to Silver Processing Complete")
