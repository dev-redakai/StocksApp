package com.stockprocessor

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import scala.util.Random

object StockDataGenerator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StockDataGenerator")
      .master("local[*]")
      .getOrCreate()

    val outputPath = if (args.length > 0) args(0) else "input/sample_data"
    val numRecords = if (args.length > 1) args(1).toInt else 1000

    generateSampleData(spark, outputPath, numRecords)
    spark.stop()
  }

  def generateSampleData(spark: SparkSession, outputPath: String, numRecords: Int = 1000): Unit = {
    import spark.implicits._

    println(s"Generating $numRecords sample records...")

    val stockNames = Seq("AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA", "NFLX")
    val currentTime = System.currentTimeMillis()
    val random = new Random()

    val twoMonthsInMillis = 60L * 24 * 60 * 60 * 1000
    val intervalSize = twoMonthsInMillis / numRecords

    val sampleData = (0 until numRecords).flatMap { i =>
      stockNames.map { stock =>
        // Base interval + random jitter
        val baseOffset = i * intervalSize
        val jitter = (random.nextGaussian() * intervalSize * 0.1).toLong // 10% jitter
        val timestamp = new Timestamp(currentTime + baseOffset + jitter)
        val basePrice = stock match {
          case "AAPL" => 150.0
          case "GOOGL" => 2800.0
          case "MSFT" => 300.0
          case "TSLA" => 800.0
          case "AMZN" => 3200.0
          case "META" => 250.0
          case "NVDA" => 400.0
          case "NFLX" => 180.0
        }

        // Add realistic price movement
        val priceMovement = (random.nextGaussian() * 5) // 5% standard deviation
        val price = math.max(basePrice + priceMovement, 1.0) // Ensure positive price
        val volume = (random.nextInt(1000000) + 100000).toLong
        val marketCap = price * 1000000000L // Simple market cap calculation

        (stock, timestamp, price, volume, marketCap)
      }
    }

    val df = sampleData.toDF("stock_name", "timestamp", "stock_price", "volume", "market_cap")

    // Write as CSV
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println(s"Sample data generated successfully!")
    println(s"Location: $outputPath")
    println(s"Records: ${df.count()}")

    // Show sample
    println("\n=== Sample Data ===")
    df.show(10)
  }
}