# Stock Data Processor

[![Scala Version](https://img.shields.io/badge/Scala-2.12.17-red)](https://scala-lang.org)
[![Spark Version](https://img.shields.io/badge/Spark-3.4.1-orange)](https://spark.apache.org)
[![Build Tool](https://img.shields.io/badge/Build-SBT-blue)](https://scala-sbt.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](LICENSE)

A high-performance, scalable Apache Spark application for real-time and batch processing of stock market data. Built with Scala and designed for processing minute-by-minute stock data to generate comprehensive analytics across multiple time horizons.

## 🚀 Features

### Core Functionality
- **Multi-timeframe Analysis**: Process stock data across configurable periods (10-min default), hourly, daily, and monthly aggregations
- **Real-time Processing**: Streaming data processing with configurable watermarks and triggers
- **Batch Processing**: Historical data analysis with optimized performance
- **Advanced Metrics**: OHLC data, volatility calculations, price change analytics, and volume analysis

### Technical Highlights
- **Scalable Architecture**: Built on Apache Spark for distributed computing
- **Memory Optimized**: Efficient data structures and caching strategies
- **Fault Tolerant**: Robust error handling and recovery mechanisms
- **Production Ready**: Comprehensive logging, monitoring, and deployment configurations

### Analytics Capabilities
- **Price Analysis**: Open, High, Low, Close (OHLC) calculations
- **Time-based Insights**: `high_till_time`, `low_till_time` tracking when extremes occurred
- **Volatility Metrics**: Standard deviation-based volatility calculations
- **Volume Analytics**: Aggregated trading volume across time periods
- **Performance Metrics**: Price changes and percentage movements

## 📊 Sample Output

```
+----------+-------------------+-------------------+----------------+----------+----------+----------+----------+---------+-----------+---------+-------------+------------+-------------------+----------+------------+-------------------+
|stock_name|period_start       |period_end         |aggregation_type|open_price|close_price|high_price|low_price |avg_price|high_till_time|low_till_time|total_volume|price_change|price_change_percent|volatility|record_count|processed_at       |
+----------+-------------------+-------------------+----------------+----------+----------+----------+----------+---------+-----------+---------+-------------+------------+-------------------+----------+------------+-------------------+
|AAPL      |2024-01-01 09:30:00|2024-01-01 09:40:00|10min           |150.25    |151.50    |152.00    |149.80    |150.89   |09:35:23   |09:32:15 |1250000      |1.25        |0.83               |0.65      |10          |2024-01-01 10:00:00|
|GOOGL     |2024-01-01 09:30:00|2024-01-01 09:40:00|10min           |2800.50   |2810.25   |2815.75   |2795.30   |2805.45  |09:37:42   |09:31:58 |875000       |9.75        |0.35               |5.23      |10          |2024-01-01 10:00:00|
+----------+-------------------+-------------------+----------------+----------+----------+----------+----------+---------+-----------+---------+-------------+------------+-------------------+----------+------------+-------------------+
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Data Source   │───▶│  Spark Streaming │───▶│   Aggregation       │
│                 │    │     Engine       │    │     Engine          │
│ • CSV Files     │    │                  │    │                     │
│ • Kafka Stream  │    │ • Watermarking   │    │ • 10-min windows    │
│ • File System   │    │ • Checkpointing  │    │ • Hourly analysis   │
└─────────────────┘    └──────────────────┘    │ • Daily summaries   │
                                               │ • Monthly reports   │
                                               └─────────────────────┘
                                                          │
                                               ┌─────────────────────┐
                                               │    Output Sinks     │
                                               │                     │
                                               │ • Parquet Files     │
                                               │ • CSV Reports       │
                                               │ • Console Output    │
                                               │ • Database Tables   │
                                               └─────────────────────┘
```

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| **Language** | Scala | 2.12.17 |
| **Processing Engine** | Apache Spark | 3.4.1 |
| **Build Tool** | SBT | 1.9.6 |
| **Configuration** | Typesafe Config | 1.4.2 |
| **Testing** | ScalaTest | 3.2.15 |
| **Logging** | Log4j2 | 2.20.0 |

## 📋 Prerequisites

- **Java**: OpenJDK 11 or Oracle JDK 11+
- **Scala**: 2.12.x (managed by SBT)
- **Apache Spark**: 3.4.1+
- **SBT**: 1.9.6+
- **Memory**: Minimum 4GB RAM recommended
- **IDE**: IntelliJ IDEA with Scala plugin (optional)

## 🚀 Quick Start

### 1. Clone and Setup
```bash
git clone <repository-url>
cd stock-data-processor

# Compile the project
sbt compile
```

### 2. Generate Sample Data
```bash
# Generate 1000 sample records
sbt "runMain com.stockprocessor.StockDataGenerator input/sample_data 1000"
```

### 3. Process Stock Data
```bash
# Run batch processing with 10-minute windows
sbt "runMain com.stockprocessor.StockDataProcessor 10 input/sample_data output/results batch"
```

### 4. View Results
```bash
# Check output directory
ls -la output/results/

# View CSV results
head -20 output/results/batch_processed_csv/*.csv
```

## 📖 Detailed Usage

### Command Line Interface

```bash
sbt "runMain com.stockprocessor.StockDataProcessor [PERIOD_MINUTES] [INPUT_PATH] [OUTPUT_PATH] [MODE]"
```

#### Parameters:
- **PERIOD_MINUTES**: Aggregation window in minutes (default: 10)
- **INPUT_PATH**: Path to input data files (default: `input/stock_data`)
- **OUTPUT_PATH**: Path for output results (default: `output/processed_stock_data`)
- **MODE**: Processing mode - `batch`, `streaming`, or `generate` (default: `batch`)

#### Examples:

```bash
# Custom 15-minute aggregation
sbt "runMain com.stockprocessor.StockDataProcessor 15 data/stocks output/analysis batch"

# Streaming processing with 5-minute windows
sbt "runMain com.stockprocessor.StockDataProcessor 5 streaming/input streaming/output streaming"

# Generate test data with 2000 records
sbt "runMain com.stockprocessor.StockDataGenerator test_data 2000"
```

### Input Data Format

The application expects CSV files with the following schema:

```csv
stock_name,timestamp,stock_price,volume,market_cap
AAPL,2024-01-01 09:30:00,150.25,1000000,2500000000000
GOOGL,2024-01-01 09:31:00,2800.50,500000,1800000000000
MSFT,2024-01-01 09:32:00,300.75,750000,2200000000000
```

| Column | Type | Description |
|--------|------|-------------|
| `stock_name` | String | Stock symbol identifier |
| `timestamp` | Timestamp | Data point timestamp |
| `stock_price` | Double | Stock price at timestamp |
| `volume` | Long | Trading volume (optional) |
| `market_cap` | Double | Market capitalization (optional) |

### Configuration

Modify `src/main/resources/application.conf`:

```hocon
processing {
  default.period.minutes = 10
  watermark.delay = "2 minutes"
  
  input {
    path = "input/stock_data"
    format = "csv"
  }
  
  output {
    path = "output/processed_stock_data"
    format = "parquet"
    mode = "overwrite"
  }
}

spark {
  app.name = "StockDataProcessor"
  executor.memory = "2g"
  driver.memory = "1g"
  executor.cores = 2
}
```

## 🧪 Testing

### Run All Tests
```bash
sbt test
```

### Run Specific Test Suite
```bash
sbt "testOnly com.stockprocessor.StockDataProcessorTest"
```

### Test Coverage
```bash
sbt coverage test coverageReport
```

### Test Structure
```
src/test/scala/com/stockprocessor/
├── StockDataProcessorTest.scala      # Main processor tests
├── StockDataGeneratorTest.scala      # Data generation tests
├── config/
│   └── AppConfigTest.scala           # Configuration tests
└── integration/
    └── IntegrationTest.scala         # End-to-end tests
```

## 📦 Building and Deployment

### Local Development
```bash
# Compile and run locally
sbt compile
sbt run

# Create fat JAR for distribution
sbt assembly
```

### Production Deployment

#### 1. Build Deployable JAR
```bash
sbt clean assembly
# Output: target/scala-2.12/stock-data-processor-1.0.0.jar
```

#### 2. Submit to Spark Cluster
```bash
spark-submit \
  --class com.stockprocessor.StockDataProcessor \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --executor-cores 4 \
  --num-executors 8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  target/scala-2.12/stock-data-processor-1.0.0.jar \
  15 hdfs://data/stocks hdfs://output/analytics batch
```

#### 3. Docker Deployment
```dockerfile
FROM openjdk:11-jre-slim

COPY target/scala-2.12/stock-data-processor-1.0.0.jar /app/
COPY src/main/resources/application.conf /app/

WORKDIR /app
CMD ["java", "-jar", "stock-data-processor-1.0.0.jar"]
```

### Performance Tuning

#### Spark Configuration for Large Datasets
```bash
spark-submit \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=5 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=10 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  your-application.jar
```

## 📊 Monitoring and Logging

### Application Metrics
- Processing throughput (records/second)
- Memory utilization
- Task completion times
- Data quality metrics

### Log Files Location
```
logs/
├── stock-processor.log           # Application logs
├── spark-executor.log           # Spark executor logs
└── performance.log              # Performance metrics
```

### Monitoring Endpoints
- Spark UI: `http://localhost:4040`
- Application Metrics: `http://localhost:4041/metrics`

## 🐛 Troubleshooting

### Common Issues

#### OutOfMemoryError
```bash
# Increase driver memory
spark-submit --driver-memory 4g --executor-memory 8g your-app.jar

# Or in sbt
sbt -J-Xmx4g run
```

#### Slow Performance
```bash
# Enable adaptive query execution
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true

# Increase parallelism
--conf spark.default.parallelism=200
```

#### Data Skew Issues
```bash
# Enable skew join optimization
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

### Debug Mode
```bash
# Run with debug logging
sbt -Dlog4j.configuration=log4j-debug.properties run

# Enable Spark SQL debug
--conf spark.sql.debug.maxToStringFields=1000
```

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Run tests: `sbt test`
5. Format code: `sbt scalafmt`
6. Commit changes: `git commit -m 'Add amazing feature'`
7. Push to branch: `git push origin feature/amazing-feature`
8. Create Pull Request

### Code Standards
- Follow Scala style guide
- Maintain test coverage >80%
- Use meaningful variable names
- Add comprehensive documentation
- Include unit tests for new features

### Pull Request Process
1. Update README.md with details of changes
2. Update version numbers in build.sbt
3. Ensure all tests pass
4. Request review from maintainers

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support
### Reporting Bugs
When reporting bugs, please include:
1. Scala and Spark versions
2. Input data sample
3. Full error stack trace
4. Steps to reproduce
5. Expected vs actual behavior

## 🚧 Roadmap

### Version 2.0.0
- [ ] Real-time dashboard integration
- [ ] Machine learning price prediction
- [ ] Multi-asset class support
- [ ] Cloud provider integrations (AWS, GCP, Azure)

### Version 1.1.0
- [ ] WebSocket streaming support
- [ ] Enhanced error handling
- [ ] Performance optimizations
- [ ] Additional technical indicators

### Version 1.0.1
- [ ] Bug fixes and improvements
- [ ] Documentation updates
- [ ] Test coverage improvements

---

**Built with ❤️ by Manikant Goutam**

*For questions or support, please open an issue or contact our support team.*