ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.11"
ThisBuild / organization := "com.stockprocessor"

// Project definition
lazy val root = (project in file("."))
  .settings(
    name := "stock-data-processor",

    // Scala compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ),

    // Java compiler options
    javacOptions ++= Seq("-source", "11", "-target", "11"),

    // Dependencies
    libraryDependencies ++= Seq(
      // Spark Core Dependencies
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.4.1" % "provided",

      // For local development (remove % "provided" for local testing)
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",

      // Configuration
      "com.typesafe" % "config" % "1.4.2",

      // Logging
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",

      // Date/Time handling
      "joda-time" % "joda-time" % "2.12.5",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.mockito" % "mockito-core" % "5.1.1" % Test,

      // JSON processing (if needed)
//      "org.json4s" %% "json4s-native" % "4.0.4",
//      "org.json4s" %% "json4s-jackson" % "4.0.4"
    ),

    // Resolver for Spark dependencies
    resolvers ++= Seq(
      "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
      "Maven Central" at "https://repo1.maven.org/maven2/"
    ),

    // Assembly plugin settings for fat JAR
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("reference.conf") => MergeStrategy.concat
      case PathList("application.conf") => MergeStrategy.concat
      case "log4j2.properties" => MergeStrategy.first
      case x if x.endsWith(".class") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.first
      case x if x.endsWith(".xml") => MergeStrategy.first
      case _ => MergeStrategy.first
    },

    // JAR naming
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",

    // Main class for assembly
    assembly / mainClass := Some("com.stockprocessor.StockDataProcessor"),

    // Test settings
    Test / parallelExecution := false,
    Test / fork := true,

    // Runtime settings
    run / fork := true,
    run / javaOptions ++= Seq(
      "-Xms2g",
      "-Xmx4g",
      "-XX:+UseG1GC"
    )
  )
