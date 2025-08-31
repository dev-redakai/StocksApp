package com.stockprocessor.data_model

import java.sql.Timestamp

case class ProcessedStockData(
                               stock_name: String,
                               period_start: Timestamp,
                               period_end: Timestamp,
                               aggregation_type: String,
                               open_price: Double,
                               close_price: Double,
                               high_price: Double,
                               low_price: Double,
                               avg_price: Double,
                               high_till_time: Timestamp,
                               low_till_time: Timestamp,
                               total_volume: Long,
                               price_change: Double,
                               price_change_percent: Double,
                               volatility: Double,
                               record_count: Long,
                               processed_at: Timestamp
                             )