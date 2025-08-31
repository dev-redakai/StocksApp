// Assembly plugin for creating fat JARs
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.3")

// Scala formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Dependency graph
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

// Coverage reports
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")

// Native packager for distributions
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")