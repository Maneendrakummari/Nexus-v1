ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-iceberg-orders-pipeline",

    libraryDependencies ++= Seq(
      // Apache Spark SQL
      "org.apache.spark" %% "spark-sql" % "3.5.9",

      // Kafka connector for Spark Structured Streaming
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",

      // Apache Iceberg runtime for Spark 3.5 / Scala 2.12
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.12.0"
    ),

    // Run the app in a separate JVM so the Java options below apply
    Compile / run / fork := true,

    // Required on Java 17 for Spark and Arrow/Parquet memory access
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
  )