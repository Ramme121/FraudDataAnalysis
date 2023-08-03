package com.dataanalysis

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FraudDetectionRandomForestClassifier {

  def main(args: Array[String]): Unit = {

    val filename = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("FraudData.csv").getPath

    // create spark session
    val spark = SparkSession
      .builder
      .appName("FraudDetectionRandomForestClassifier")
      .master("local[8]")
      .getOrCreate()

    // read csv file
    var df = spark
      .read
      .schema(Utils.schema)
      .option("header", value = true)
      .csv(filename)

    df.show()

    //print schema
    println(df.schema)

    // Select relevant columns for the model
    val selectedCols = Seq("TotalCredit", "MonthlyPayments", "CreditScore", "CustomerAge", "Gender", "OwnsCar", "OwnsProperty", "NumberOfChildren", "Fraud")
    df = df.select(selectedCols.map(name => col(name)) : _*)

    // Drop rows with any missing values
    df = df.na.drop()

    // Split data into training and test sets
    val splits = df.randomSplit(Array(0.8, 0.2), seed = 1234)
    val training = splits(0).cache()
    val test = splits(1)

    // Create a StringIndexer to convert the "Gender" column into numerical indices
    val indexer = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndex")

    // Create a OneHotEncoder to convert the "GenderIndex" column into a binary vector representation
    val encoder = new OneHotEncoder().setInputCol("GenderIndex").setOutputCol("GenderVec")

    // Update the VectorAssembler to include the "GenderVec" column in the inputCols
    val vectorAssembler = new VectorAssembler()
      .setInputCols(
        Array(
          "TotalCredit",
          "MonthlyPayments",
          "CreditScore",
          "CustomerAge",
          "GenderVec",
          "OwnsCar",
          "OwnsProperty",
          "NumberOfChildren"
        )
      ).setOutputCol("features")

    // Create a Random Forest classifier
    val rf = new RandomForestClassifier().setLabelCol("Fraud").setFeaturesCol("features").setNumTrees(100)

    // Create a pipeline with the updated stages and Random Forest classifier
    val pipeline = new Pipeline().setStages(Array(indexer, encoder, vectorAssembler, rf))

    // Fit the pipeline on the training data
    val model = pipeline.fit(training)

    // Make predictions on the test set
    val predictions = model.transform(test)

    // Show the predicted and actual fraud labels
    predictions.select("Fraud", "prediction").show()

    // Create a BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("Fraud")

    // Calculate the AUC-ROC
    val aucRoc = evaluator.setMetricName("areaUnderROC").evaluate(predictions)
    println(s"AUC-ROC: $aucRoc")

    // Calculate the AUC-PR
    val aucPr = evaluator.setMetricName("areaUnderPR").evaluate(predictions)
    println(s"AUC-PR: $aucPr")


  }
}
