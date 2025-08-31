package com.stockprocessor.data_model

import java.sql.Timestamp

case class StockData(
                      stock_name: String,
                      timestamp: Timestamp,
                      stock_price: Double,
                      volume: Option[Long] = None,
                      market_cap: Option[Double] = None
                    )
