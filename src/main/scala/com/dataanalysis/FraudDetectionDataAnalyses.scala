package com.dataanalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FraudDetectionDataAnalyses {

  def main(args: Array[String]): Unit = {

    val filename = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("FraudData.csv").getPath

    //create spark session
    val spark = SparkSession
      .builder
      .appName("FraudDetectionDataAnalyses")
      .master("local[8]")
      .getOrCreate()

    //read csv file
    var df = spark
      .read
      .schema(Utils.schema)
      .option("header", value = true)
      .csv(filename)

    df.show()

    //print schema
    println(df.schema)

    //create temp view FraudData
    df.createOrReplaceTempView("FraudData")
    val latePaymentFraudAnalysis = spark.sql(
      "SELECT Fraud, AVG(LatePaymentAmount) AS AverageLatePaymentAmount FROM FraudData GROUP BY Fraud"
    )

    // Show the results for Late Payment Fraud Analysis
    latePaymentFraudAnalysis.show()

    val riskLevelAnalysis = df
      .withColumn("RiskLevel", when(col("RiskLevel").isNull, "No Risk").otherwise(col("RiskLevel")))
      .groupBy("RiskLevel")
      .agg(
        count("LoanID").alias("TotalLoans"),
        sum("Fraud").alias("TotalFrauds"),
        avg("AnnualIncome").alias("AvgIncome"),
        avg("CreditScore").alias("AvgCreditScore"),
        max("CustomerAge").alias("MaxCustomerAge"),
        min("CustomerAge").alias("MinCustomerAge")
      )

    // Show the results for Risk Level Analysis
    riskLevelAnalysis.show()


  }
}
