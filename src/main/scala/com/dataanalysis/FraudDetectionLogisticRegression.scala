package com.dataanalysis

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler


object FraudDetectionLogisticRegression {

  def main(args: Array[String]): Unit = {

    val filename = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("FraudData.csv").getPath

    //create spark session
    val spark = SparkSession
      .builder
      .appName("FraudDetectionLogisticRegression")
      .master("local[8]")
      .getOrCreate()

    //read data from csv
    var df = spark
      .read
      .schema(Utils.schema)
      .option("header", value = true)
      .csv(filename)

    df.show()

    // print schema
    println(df.schema)

    // Select relevant columns for the model
    val selectedCols = Seq("TotalCredit", "MonthlyPayments", "CreditScore", "CustomerAge", "Gender", "OwnsCar", "OwnsProperty", "NumberOfChildren", "Fraud")
    df = df.select(selectedCols.map(name => col(name)) : _*)

    // Drop rows with any missing values
    df = df.na.drop()

    // VectorAssembler to combine features into a single vector column
    val assembler = new VectorAssembler()
      .setInputCols(Array("TotalCredit", "MonthlyPayments", "CreditScore", "CustomerAge", "OwnsCar", "OwnsProperty", "NumberOfChildren"))
      .setOutputCol("features")

    // Logistic Regression model
    val lr = new LogisticRegression().setLabelCol("Fraud").setFeaturesCol("features")

    // Pipeline for assembling features and training the model
    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    // Split data into training and test sets
    val splits = df.randomSplit(Array(0.8, 0.2), seed = 1234)
    val training = splits(0).cache()
    val test = splits(1)

    // Train the model using the training data
    val model = pipeline.fit(training)

    // Make predictions on the test set
    val predictions = model.transform(test)

    // Display the predictions
    predictions.select("Fraud", "prediction").show()

    val evaluatorMulticlass = new MulticlassClassificationEvaluator().setLabelCol("Fraud").setPredictionCol("prediction").setMetricName("accuracy")

    // Calculate accuracy
    val accuracy = evaluatorMulticlass.evaluate(predictions)

    println(s"Accuracy: $accuracy")

    // Evaluate the model using BinaryClassificationEvaluator for both AUC and accuracy
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("Fraud").setRawPredictionCol("rawPrediction")
    val auRoc = evaluator.setMetricName("areaUnderROC").evaluate(predictions)
    val auPr = evaluator.setMetricName("areaUnderPR").evaluate(predictions)

    // Create a DataFrame to store the evaluation results
    val evaluation_result = spark.createDataFrame(Seq((auRoc, "Area Under ROC"), (auPr, "Area Under PR"))).toDF(Seq("Value", "Metric") : _*)

    // Show the evaluation results
    evaluation_result.show()


  }
}
