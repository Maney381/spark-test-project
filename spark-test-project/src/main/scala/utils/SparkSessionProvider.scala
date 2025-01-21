package utils

import org.apache.spark.sql.SparkSession


object SparkSessionProvider {
    // 1. Create SparkSession
    lazy val spark = SparkSession.builder()
      .appName("SparkSession")
      .master("local[*]")  // For local testing
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
}
