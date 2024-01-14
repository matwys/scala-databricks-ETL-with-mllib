// Databricks notebook source
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vectors}
import org.apache.spark.ml.regression.{LinearRegression}

// COMMAND ----------

val cruiseShipDataPath = "/mnt/cruise-ship/gold/cruise_ship_ml"
val cruiseShipData = spark.read.format("delta").load(cruiseShipDataPath)

// COMMAND ----------

display(cruiseShipData)

// COMMAND ----------

val indexer = new StringIndexer().setInputCol("CRUISE_LINE").setOutputCol("CRUISE_LINE_INDEXED")

// COMMAND ----------

val indexerCruiseShipData = indexer.fit(cruiseShipData)

// COMMAND ----------

val indexedCruiseShipData = indexerCruiseShipData.transform(cruiseShipData)

// COMMAND ----------

display(indexedCruiseShipData)

// COMMAND ----------

indexedCruiseShipData.printSchema

// COMMAND ----------

val assembler = new VectorAssembler().setInputCols(Array("CRUISE_LINE_INDEXED","AGE","TONNAGE","PASSENGERS","LENGTH", "CABINS", "PASSENGER_DENSITY")).setOutputCol("features")  

// COMMAND ----------

val vectoredCruiseShipData = assembler.transform(indexedCruiseShipData)

// COMMAND ----------

display(vectoredCruiseShipData)

// COMMAND ----------

val splits  = vectoredCruiseShipData.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0)
val test = splits(1)

// COMMAND ----------

val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("CREW")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// COMMAND ----------

val lrModel = lr.fit(training)

// COMMAND ----------

val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")

// COMMAND ----------

display(lrModel.transform(test).select("CREW","prediction"))

// COMMAND ----------


