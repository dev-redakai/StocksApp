package com.stockprocessor

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

class StockDataProcessorTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("StockProcessorTest")
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  test("Stock data schema should be correct") {
    val schema = StockDataProcessor.getStockDataSchema()

    assert(schema.fields.length == 5)
    assert(schema.fieldNames.contains("stock_name"))
    assert(schema.fieldNames.contains("timestamp"))
    assert(schema.fieldNames.contains("stock_price"))
  }

  test("Sample data generation should work") {
    val tempDir = "target/test_data"

    StockDataGenerator.generateSampleData(spark, tempDir, 100000)

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tempDir)

    assert(df.count() > 0)
    assert(df.columns.contains("stock_name"))
    assert(df.columns.contains("stock_price"))
  }

  test("Periodic aggregation should work correctly") {
    // Create test data
    val testData = Seq(
      ("AAPL", Timestamp.valueOf("2024-01-01 10:00:00"), 150.0, 1000L, 1500000000000L),
      ("AAPL", Timestamp.valueOf("2024-01-01 10:01:00"), 151.0, 1100L, 1500000000000L),
      ("AAPL", Timestamp.valueOf("2024-01-01 10:02:00"), 149.0, 900L, 1500000000000L)
    ).toDF("stock_name", "timestamp", "stock_price", "volume", "market_cap")

    val result = StockDataProcessor.generatePeriodicAggregation(testData, 10)

    assert(result.count() > 0)
    assert(result.columns.contains("high_price"))
    assert(result.columns.contains("low_price"))
    assert(result.columns.contains("aggregation_type"))
  }

  override def finalize(): Unit = {
    spark.stop()
    super.finalize()
  }
}