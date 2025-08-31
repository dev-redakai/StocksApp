package com.stockprocessor.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  case class AppConfiguration(
                               appName: String,
                               defaultPeriodMinutes: Int,
                               inputPath: String,
                               outputPath: String,
                               watermarkDelay: String,
                               triggerInterval: String
                             )

  def load(): AppConfiguration = {
    val config: Config = ConfigFactory.load()

    AppConfiguration(
      appName = config.getString("app.name"),
      defaultPeriodMinutes = config.getInt("processing.default.period.minutes"),
      inputPath = config.getString("processing.input.path"),
      outputPath = config.getString("processing.output.path"),
      watermarkDelay = config.getString("processing.watermark.delay"),
      triggerInterval = config.getString("streaming.trigger.interval")
    )
  }
}