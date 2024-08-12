package main

import org.apache.spark.sql.{SaveMode, SparkSession}
import ETL.ETL

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("AirOne")
      .master("local[*]")
      .config("spark.driver.extraClassPath", "/Users/vishal/Downloads/ssqljdbc_12.6")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .getOrCreate()

    val path = "/Users/vishal/Downloads/RAW/CorporateManagement_Support/CorporateFinance/transactionaldata/PRA/SLST/SLSVALID/2024/07"
    val readPath = s"$path/*/*"

    // Read the JSON data into a DataFrame
    val df = spark.read.json(readPath)

    // Flatten DataFrames
    val legDF = ETL.flattenLeg(df)
    val segmentDF = ETL.flattenSegment(df)
    val joinDF = ETL.joinDataframes(legDF, segmentDF)
    joinDF.show()

    // Write DataFrame to Delta Lake
    val deltaTablePath = "/Users/vishal/Desktop/delta/airlineData"
    joinDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .option("mergeSchema", "true")
      .save(deltaTablePath)


    val jdbcUrl = "jdbc:sqlserver://Localhost:1433;databaseName=master"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "SA")
    connectionProperties.setProperty("password", "MyStrongPass123")

    // Write to SQL Server
    val targetTable = "airlineData"
    joinDF.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", targetTable)
      .option("user", connectionProperties.getProperty("user"))
      .option("password", connectionProperties.getProperty("password"))
      .mode(SaveMode.Append)
      .save()

    // Stop the SparkSession
    spark.stop()
  }
}
