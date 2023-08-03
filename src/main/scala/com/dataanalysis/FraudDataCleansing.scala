package com.dataanalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FraudDataCleansing {

  def main(args: Array[String]): Unit = {

    val filename = if (args.length > 0) args(0) else getClass.getClassLoader.getResource("FraudData.csv").getPath

    //create spark session
    val spark = SparkSession
      .builder
      .appName("FraudDataCleansing")
      .master("local[8]")
      .getOrCreate()

    //read csv data
    var df = spark
      .read
      .schema(Utils.schema)
      .option("header", true)
      .csv(filename)

    df.show()

    //print schema
    println(df.schema)

    // Filling missing values in the "TotalCredit" column with the mean value
    val meanTotalCredit = df.select("TotalCredit").agg(Map("TotalCredit" -> "mean")).head().getDouble(0)

    df = df.na.fill(meanTotalCredit, Array("TotalCredit"))

    // Converting the "AnnualIncome" column to IntegerType (replace 50000 with the desired integer value)
    df = df.withColumn("AnnualIncome", lit(50000).cast(IntegerType))

    // Feature Selection (Assuming you want to use only selected columns for the model)
    val selectedColumns = Seq("TotalCredit", "MonthlyPayments", "CreditScore", "CustomerAge", "Gender", "OwnsCar", "OwnsProperty", "NumberOfChildren", "Fraud")

    df = df.select(selectedColumns.map(name => col(name)): _*)

    // Converting 'CreditScore' and 'CustomerAge' columns to IntegerType
    df = df.withColumn("CreditScore", col("CreditScore").cast(IntegerType))
    df = df.withColumn("CustomerAge", col("CustomerAge").cast(IntegerType))

    // Converting 'OwnsCar', 'OwnsProperty', and 'NumberOfChildren' columns to IntegerType
    df = df.withColumn("OwnsCar", col("OwnsCar").cast(IntegerType))
    df = df.withColumn("OwnsProperty", col("OwnsProperty").cast(IntegerType))
    df = df.withColumn("NumberOfChildren", col("NumberOfChildren").cast(IntegerType))

    df.write.mode("overwrite").format("csv").save("bank_fraud_transformed")

  }
}
