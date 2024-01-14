// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val cruiseShipDataPath = "/mnt/cruise-ship/silver/cruise_ship"
val cruiseShipData = spark.read.format("delta").load(cruiseShipDataPath)

// COMMAND ----------

display(cruiseShipData)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cruiser Line Statistics:

// COMMAND ----------

val cruiceLineStatistics = cruiseShipData.groupBy(cruiseShipData("CRUISE_LINE")).agg(
      count(lit(1)).as("num_of_ships"),
      avg("AGE").as("avg_age"),
      max("PASSENGERS").as("max_passengers"),
      min("PASSENGERS").as("min_passengers")
      )

// COMMAND ----------

display(cruiceLineStatistics)

// COMMAND ----------

cruiceLineStatistics.write.format("delta").mode("overwrite").option("path","/mnt/cruise-ship/gold/cruice_line_statistics").saveAsTable("shipinfo.cruice_line_statistics")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Full Data for Machine Learning:

// COMMAND ----------

cruiseShipData.write.format("delta").mode("overwrite").option("path","/mnt/cruise-ship/gold/cruise_ship_ml").saveAsTable("shipinfo.cruise_ship_ml")

// COMMAND ----------

dbutils.notebook.exit("Silver to Gold Processing Complete")
