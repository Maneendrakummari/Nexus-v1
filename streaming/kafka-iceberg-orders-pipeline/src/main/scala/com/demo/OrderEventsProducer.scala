package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

/**
 * FILE 1 of 3 - events -> Kafka
 *
 * Publishes the 5 micro-batch files (data/micro_batches/order_events_00N.json) to the
 * Kafka topic "retail", one file at a time, waiting between files so that every
 * file lands in its own Spark Structured Streaming micro-batch (= 5 Iceberg snapshots).
 *
 * Kafka key = order_id, or event_id when order_id is null (same rule as order_events_kafka_keyed.txt).
 *
 * Run AFTER OrderEventsStreaming is already running.
 */
object OrderEventsProducer {

  // Local Kafka broker (docker-compose.yml)
  val BootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  val Topic = "retail"

  // Folder with the 5 micro-batch files (relative to the project root)
  val MicroBatchDir = "data/micro_batches"

  // Must be bigger than the streaming trigger (10 seconds) so one file = one micro-batch
  val GapSeconds = 20

  // How to print each published batch on the console: "table" or "json"
  val ShowFormat = "table"

  val EventFields = Seq("event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "event_type", "event_time", "ingest_time", "sales_channel", "discount_amount")

  def main(args: Array[String]): Unit = {
    // Windows: folder containing bin\winutils.exe and bin\hadoop.dll
    System.setProperty("hadoop.home.dir", "C:/hadoop")

    val spark = SparkSession.builder()
      .appName("OrderEventsProducer")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val files = Option(new File(MicroBatchDir).listFiles())
      .getOrElse(Array.empty[File])
      .filter(_.getName.endsWith(".json"))
      .sortBy(_.getName)

    require(files.nonEmpty, s"No micro-batch files found in $MicroBatchDir (run from the project root)")

    files.zipWithIndex.foreach { case (file, i) =>
      // Each line of the file is one JSON event
      val lines = spark.read.textFile(file.toURI.toString)

      // Kafka needs a key and a value column
      val messages = lines
        .filter(trim(col("value")) =!= "")
        .select(
          coalesce(
            get_json_object(col("value"), "$.order_id"),
            get_json_object(col("value"), "$.event_id")
          ).as("key"),
          col("value")
        )
        .cache()

      val count = messages.count()

      // Show what is about to be published
      println(s"\n===== MICRO-BATCH ${i + 1}/${files.length}: ${file.getName} ($count events) =====")
      if (ShowFormat == "json") {
        messages.select(concat_ws(" | ", col("key"), col("value")).as("key | json")).show(100, truncate = false)
      } else {
        messages
          .select(col("key"), json_tuple(col("value"), EventFields: _*))
          .toDF(Seq("kafka_key") ++ EventFields: _*)
          .show(100, truncate = false)
      }

      messages.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BootstrapServers)
        .option("topic", Topic)
        .save()

      messages.unpersist()

      println(s"PUBLISHED micro-batch ${i + 1}/${files.length}: ${file.getName} -> $count events to topic $Topic")

      if (i < files.length - 1) {
        println(s"Waiting $GapSeconds seconds so the stream processes this batch on its own ...")
        Thread.sleep(GapSeconds * 1000L)
      }
    }

    println("All micro-batches published.")
    spark.stop()
  }
}