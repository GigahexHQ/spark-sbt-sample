package com.gigahex.sample

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkUtils {

  def getLocalSession(appName: String): SparkSession =
    SparkSession.builder().getOrCreate()

  def readCSV(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(path)

}
