package com.stockprocessor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import com.stockprocessor.config.AppConfig
import com.stockprocessor.data_model.StockData
import com.stockprocessor.data_model.ProcessedStockData
import java.sql.Timestamp
import org.apache.spark.sql.Row

object StockDataProcessor {

  def main(args: Array[String]): Unit = {
    println("Starting Stock Data Processor...")

    // Initialize Spark Session
    val spark = createSparkSession()
    import spark.implicits._

    // Load configuration
    val config = AppConfig.load()

    // Parse command line arguments
    val periodWindowMinutes = if (args.length > 0) args(0).toInt else config.defaultPeriodMinutes
    val inputPath = if (args.length > 1) args(1) else config.inputPath
    val outputPath = if (args.length > 2) args(2) else config.outputPath
    val processingMode = if (args.length > 3) args(3) else "batch"

    println(s"Configuration:")
    println(s"- Period Window: $periodWindowMinutes minutes")
    println(s"- Input Path: $inputPath")
    println(s"- Output Path: $outputPath")
    println(s"- Processing Mode: $processingMode")

    try {
      processingMode.toLowerCase match {
        case "batch" =>
          processHistoricalData(spark, inputPath, outputPath, periodWindowMinutes)
        case "streaming" =>
          processStreamingData(spark, inputPath, outputPath, periodWindowMinutes)
        case "generate" =>
          StockDataGenerator.generateSampleData(spark, inputPath)
        case _ =>
          println("Invalid processing mode. Use: batch, streaming, or generate")
      }
    } catch {
      case e: Exception =>
        println(s"Error during processing: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("StockDataProcessor")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  def processHistoricalData(spark: SparkSession, inputPath: String, outputPath: String, periodWindowMinutes: Int): Unit = {
    import spark.implicits._

    println("Processing historical data...")

    // Read stock data
    val stockDF = readStockData(spark, inputPath)

    if (stockDF.isEmpty) {
      println("No data found in input path. Please check the path and data format.")
      return
    }

    println(s"Loaded ${stockDF.count()} records")
    stockDF.show(5)

    // Generate all aggregations
    val periodicAgg = generatePeriodicAggregation(stockDF, periodWindowMinutes)
    val hourlyAgg = generateHourlyAggregation(stockDF)
    val dailyAgg = generateDailyAggregation(stockDF)
    val monthlyAgg = generateMonthlyAggregation(stockDF)

    // Union all aggregations
    val finalResult = periodicAgg
      .union(hourlyAgg)
      .union(dailyAgg)
      .union(monthlyAgg)
      .orderBy("stock_name", "aggregation_type", "period_start")

    // Write results
    finalResult.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .parquet(s"$outputPath/batch_processed")

    // Also save as CSV for easy viewing
    finalResult.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/batch_processed_csv")

    println("=== Processing Complete ===")
    println(s"Results saved to: $outputPath")

    // Show sample results
    println("=== Sample Results ===")
    finalResult.show(20, truncate = false)

    // Show summary statistics
    println("=== Summary Statistics ===")
    finalResult.groupBy("aggregation_type", "stock_name")
      .agg(
        count("*").as("total_periods"),
        avg("avg_price").as("overall_avg_price"),
        max("high_price").as("max_high"),
        min("low_price").as("min_low")
      )
      .select("stock_name", "total_periods", "aggregation_type", "min_low", "max_high", "overall_avg_price")
      .orderBy("stock_name", "aggregation_type", "total_periods")
      .show()
  }

  def processStreamingData(spark: SparkSession, inputPath: String, outputPath: String, periodWindowMinutes: Int): Unit = {
    import spark.implicits._

    println("Starting streaming processing...")

    // Read streaming data
    val streamingDF = spark.readStream
      .format("csv")
      .option("header", "true")
      .option("maxFilesPerTrigger", "1")
      .schema(getStockDataSchema())
      .load(inputPath)

    // Process streaming aggregations
    val streamingAggregation = generateStreamingAggregation(streamingDF, periodWindowMinutes)

    val query = streamingAggregation.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime(s"${periodWindowMinutes} minutes"))
      .start()

    println("Streaming query started. Press Ctrl+C to stop...")
    query.awaitTermination()
  }

  def readStockData(spark: SparkSession, inputPath: String): DataFrame = {
    try {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .schema(getStockDataSchema())
        .load(inputPath)
        .filter(col("stock_price").isNotNull && col("stock_price") > 0)
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    } catch {
      case e: Exception =>
        println(s"Error reading data from $inputPath: ${e.getMessage}")
        spark.emptyDataFrame
    }
  }

  def getStockDataSchema(): StructType = {
    StructType(Seq(
      StructField("stock_name", StringType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("stock_price", DoubleType, nullable = false),
      StructField("volume", LongType, nullable = true),
      StructField("market_cap", DoubleType, nullable = true)
    ))
  }

  // Rest of the aggregation methods remain the same...
  // [Include all the aggregation methods from the previous code]

  def generatePeriodicAggregation(df: DataFrame, periodMinutes: Int): DataFrame = {
    val windowSpec = window(col("timestamp"), s"${periodMinutes} minutes")

    df.groupBy(col("stock_name"), windowSpec.as("time_window"))
      .agg(
        min("timestamp").as("period_start"),
        max("timestamp").as("period_end"),
        first("stock_price").as("open_price"),
        last("stock_price").as("close_price"),
        max("stock_price").as("high_price"),
        min("stock_price").as("low_price"),
        avg("stock_price").as("avg_price"),
        sum(coalesce(col("volume"), lit(0))).as("total_volume"),
        count("*").as("record_count"),
        collect_list(struct(col("stock_price"), col("timestamp"))).as("price_timestamps")
      )
      .withColumn("high_till_time", getHighTillTime(col("price_timestamps")))
      .withColumn("low_till_time", getLowTillTime(col("price_timestamps")))
      .withColumn("price_change", col("close_price") - col("open_price"))
      .withColumn("price_change_percent",
        when(col("open_price") =!= 0, (col("price_change") / col("open_price")) * 100)
          .otherwise(0.0))
      .withColumn("volatility", calculateVolatility(col("price_timestamps")))
      .withColumn("aggregation_type", lit(s"${periodMinutes}min"))
      .withColumn("processed_at", current_timestamp())
      .drop("price_timestamps", "time_window")
  }

  // Include all other methods from the original code...
  // [Copy all other methods: generateHourlyAggregation, generateDailyAggregation, etc.]
  def generateHourlyAggregation(df: DataFrame): DataFrame = {
    df.groupBy(col("stock_name"),
        date_trunc("hour", col("timestamp")).as("hour_start"))
      .agg(
        col("hour_start").as("period_start"),
        (col("hour_start") + expr("INTERVAL 1 HOUR") - expr("INTERVAL 1 SECOND")).as("period_end"),
        first("stock_price").as("open_price"),
        last("stock_price").as("close_price"),
        max("stock_price").as("high_price"),
        min("stock_price").as("low_price"),
        avg("stock_price").as("avg_price"),
        sum(coalesce(col("volume"), lit(0))).as("total_volume"),
        count("*").as("record_count"),
        collect_list(struct(col("stock_price"), col("timestamp"))).as("price_timestamps")
      )
      .withColumn("high_till_time", getHighTillTime(col("price_timestamps")))
      .withColumn("low_till_time", getLowTillTime(col("price_timestamps")))
      .withColumn("price_change", col("close_price") - col("open_price"))
      .withColumn("price_change_percent",
        when(col("open_price") =!= 0, (col("price_change") / col("open_price")) * 100)
          .otherwise(0.0))
      .withColumn("volatility", calculateVolatility(col("price_timestamps")))
      .withColumn("aggregation_type", lit("hourly"))
      .withColumn("processed_at", current_timestamp())
      .drop("price_timestamps", "hour_start")
  }

  def generateDailyAggregation(df: DataFrame): DataFrame = {
    df.groupBy(col("stock_name"),
        date_trunc("day", col("timestamp")).as("day_start"))
      .agg(
        col("day_start").as("period_start"),
        (col("day_start") + expr("INTERVAL 1 DAY") - expr("INTERVAL 1 SECOND")).as("period_end"),
        first("stock_price").as("open_price"),
        last("stock_price").as("close_price"),
        max("stock_price").as("high_price"),
        min("stock_price").as("low_price"),
        avg("stock_price").as("avg_price"),
        sum(coalesce(col("volume"), lit(0))).as("total_volume"),
        count("*").as("record_count"),
        collect_list(struct(col("stock_price"), col("timestamp"))).as("price_timestamps")
      )
      .withColumn("high_till_time", getHighTillTime(col("price_timestamps")))
      .withColumn("low_till_time", getLowTillTime(col("price_timestamps")))
      .withColumn("price_change", col("close_price") - col("open_price"))
      .withColumn("price_change_percent",
        when(col("open_price") =!= 0, (col("price_change") / col("open_price")) * 100)
          .otherwise(0.0))
      .withColumn("volatility", calculateVolatility(col("price_timestamps")))
      .withColumn("aggregation_type", lit("daily"))
      .withColumn("processed_at", current_timestamp())
      .drop("price_timestamps", "day_start")
  }

  def generateMonthlyAggregation(df: DataFrame): DataFrame = {
    df.groupBy(col("stock_name"),
        date_trunc("month", col("timestamp")).as("month_start"))
      .agg(
        col("month_start").as("period_start"),
        (col("month_start") + expr("INTERVAL 1 MONTH") - expr("INTERVAL 1 SECOND")).as("period_end"),
        first("stock_price").as("open_price"),
        last("stock_price").as("close_price"),
        max("stock_price").as("high_price"),
        min("stock_price").as("low_price"),
        avg("stock_price").as("avg_price"),
        sum(coalesce(col("volume"), lit(0))).as("total_volume"),
        count("*").as("record_count"),
        collect_list(struct(col("stock_price"), col("timestamp"))).as("price_timestamps")
      )
      .withColumn("high_till_time", getHighTillTime(col("price_timestamps")))
      .withColumn("low_till_time", getLowTillTime(col("price_timestamps")))
      .withColumn("price_change", col("close_price") - col("open_price"))
      .withColumn("price_change_percent",
        when(col("open_price") =!= 0, (col("price_change") / col("open_price")) * 100)
          .otherwise(0.0))
      .withColumn("volatility", calculateVolatility(col("price_timestamps")))
      .withColumn("aggregation_type", lit("monthly"))
      .withColumn("processed_at", current_timestamp())
      .drop("price_timestamps", "month_start")
  }

  def generateStreamingAggregation(df: DataFrame, periodMinutes: Int): DataFrame = {
    val windowSpec = window(col("timestamp"), s"${periodMinutes} minutes")

    df.withWatermark("timestamp", s"${periodMinutes * 2} minutes")
      .groupBy(col("stock_name"), windowSpec.as("time_window"))
      .agg(
        min("timestamp").as("period_start"),
        max("timestamp").as("period_end"),
        first("stock_price").as("open_price"),
        last("stock_price").as("close_price"),
        max("stock_price").as("high_price"),
        min("stock_price").as("low_price"),
        avg("stock_price").as("avg_price"),
        sum(coalesce(col("volume"), lit(0))).as("total_volume"),
        count("*").as("record_count")
      )
      .withColumn("price_change", col("close_price") - col("open_price"))
      .withColumn("price_change_percent",
        when(col("open_price") =!= 0, (col("price_change") / col("open_price")) * 100)
          .otherwise(0.0))
      .withColumn("aggregation_type", lit(s"${periodMinutes}min_streaming"))
      .withColumn("processed_at", current_timestamp())
      .drop("time_window")
  }

  // UDF functions
  def getHighTillTime(priceTimestamps: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    udf((prices: Seq[Row]) => {
      if (prices.nonEmpty) {
        val maxPrice = prices.map(_.getAs[Double]("stock_price")).max
        prices.find(_.getAs[Double]("stock_price") == maxPrice)
          .map(_.getAs[Timestamp]("timestamp"))
          .orNull
      } else null
    }).apply(priceTimestamps)
  }

  def getLowTillTime(priceTimestamps: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    udf((prices: Seq[Row]) => {
      if (prices.nonEmpty) {
        val minPrice = prices.map(_.getAs[Double]("stock_price")).min
        prices.find(_.getAs[Double]("stock_price") == minPrice)
          .map(_.getAs[Timestamp]("timestamp"))
          .orNull
      } else null
    }).apply(priceTimestamps)
  }

  def calculateVolatility(priceTimestamps: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    udf((prices: Seq[Row]) => {
      if (prices.length > 1) {
        val priceValues = prices.map(_.getAs[Double]("stock_price"))
        val mean = priceValues.sum / priceValues.length
        val variance = priceValues.map(price => math.pow(price - mean, 2)).sum / priceValues.length
        math.sqrt(variance)
      } else 0.0
    }).apply(priceTimestamps)
  }

}