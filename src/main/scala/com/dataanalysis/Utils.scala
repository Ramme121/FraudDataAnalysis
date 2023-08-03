package com.dataanalysis

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Utils {
  val schema: StructType = StructType(
    Seq(
      StructField("LoanID", IntegerType, nullable = true),
      StructField("Fraud", IntegerType, nullable = true),
      StructField("Loan Provider", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("OwnsCar", IntegerType, nullable = true),
      StructField("OwnsProperty", IntegerType, nullable = true),
      StructField("NumberOfChildren", IntegerType, nullable = true),
      StructField("AnnualIncome", DoubleType, nullable = true),
      StructField("TotalCredit", DoubleType, nullable = true),
      StructField("MonthlyPayments", DoubleType, nullable = true),
      StructField("SecurityDeposit", DoubleType, nullable = true),
      StructField("LatePaymentAmount", IntegerType, nullable = true),
      StructField("Occupation", StringType, nullable = true),
      StructField("Education", StringType, nullable = true),
      StructField("RiskLevel", StringType, nullable = true),
      StructField("MartialStatus", StringType, nullable = true),
      StructField("HouseType", StringType, nullable = true),
      StructField("NumberOfAccounts", IntegerType, nullable = true),
      StructField("CreditScore", IntegerType, nullable = true),
      StructField("CustomerAge", IntegerType, nullable = true),
      StructField("IncomeTaxDefault", IntegerType, nullable = true),
      StructField("NumberOfBankAccounts", IntegerType, nullable = true),
      StructField("ApplicationDay", StringType, nullable = true),
      StructField("HourOfDayApplied", IntegerType, nullable = true),
      StructField("ExperianRating", DoubleType, nullable = true)
    )
  )

}
