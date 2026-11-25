package com.demo

import org.apache.spark.sql.SparkSession

/**
 * FILE 3 of 3 - verify what is stored in Iceberg
 *
 * Expected for the supplied data:
 *   bronze = 62, silver = 47, quarantine = 13, orders = 47, duplicate_audit = 5 (5 batches)
 */
object ReadOrderTables {

  // environment variable, else the local Windows default
  val Warehouse: String = sys.env.getOrElse("WAREHOUSE", "file:///C:/iceberg/warehouse")

  val Bronze = "local.orders.bronze_order_events"
  val Silver = "local.orders.silver_order_events"
  val Quarantine = "local.orders.quarantine_order_events"
  val Orders = "local.orders.orders"
  val DuplicateAudit = "local.orders.duplicate_audit"

  val HistoricalOrders = "('ORD000011', 'ORD000041', 'ORD000066')"

  def main(args: Array[String]): Unit = {
    if (sys.props.getOrElse("os.name", "").toLowerCase.contains("win")) {
      System.setProperty("hadoop.home.dir", "C:/hadoop")
    }

    val builder = SparkSession.builder()
      .appName("ReadOrderTables")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", Warehouse)
      .config("spark.sql.session.timeZone", "UTC")

    if (!sys.props.contains("spark.master")) builder.master("local[*]")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 1. Row counts and reconciliation
    println("ROW COUNTS:")
    spark.sql(s"""
      SELECT
        (SELECT count(*) FROM $Bronze)                      AS bronze_rows,
        (SELECT count(*) FROM $Silver)                      AS silver_rows,
        (SELECT count(DISTINCT event_id) FROM $Silver)      AS silver_distinct_event_ids,
        (SELECT count(*) FROM $Quarantine)                  AS quarantine_rows,
        (SELECT count(*) FROM $Orders)                      AS orders_rows,
        (SELECT count(*) FROM $DuplicateAudit)              AS duplicate_audit_rows
    """).show(truncate = false)

    // 2. Bronze rows per micro-batch (should be 13, 13, 13, 13, 10)
    println("BRONZE ROWS PER MICRO-BATCH:")
    spark.sql(s"SELECT batch_id, count(*) AS rows FROM $Bronze GROUP BY batch_id ORDER BY batch_id")
      .show(truncate = false)

    // 3. Rejected records and the reasons
    println("QUARANTINE - REJECTED RECORDS AND REASONS:")
    spark.sql(s"""
      SELECT batch_id, event_id, order_id, event_type, reject_reasons, event_ts, watermark_ts
      FROM $Quarantine
      ORDER BY batch_id, event_id
    """).show(100, truncate = false)

    println("QUARANTINE - COUNT PER REASON:")
    spark.sql(s"""
      SELECT reason, count(*) AS records
      FROM (SELECT explode(split(reject_reasons, '; ')) AS reason FROM $Quarantine)
      GROUP BY reason
      ORDER BY records DESC, reason
    """).show(truncate = false)

    // 4. Duplicates: what happened to each duplicated event_id (5 expected)
    println("DUPLICATE AUDIT - ONE ROW PER DUPLICATED event_id:")
    spark.sql(s"""
      SELECT batch_id, event_id, resolution, copies, first_ingest_ts, last_ingest_ts
      FROM $DuplicateAudit
      ORDER BY batch_id, event_id
    """).show(100, truncate = false)

    // 5. Valid records
    println("SILVER - VALID RECORDS:")
    spark.sql(s"SELECT * FROM $Silver ORDER BY event_ts, event_id").show(100, truncate = false)

    // 6. Orders: current state, and the 3 historical orders before and after MERGE INTO
    println("ORDERS - COUNT PER STATUS:")
    spark.sql(s"SELECT order_status, count(*) AS orders FROM $Orders GROUP BY order_status ORDER BY order_status")
      .show(truncate = false)

    val firstSnapshot = spark.sql(s"SELECT snapshot_id FROM $Orders.snapshots ORDER BY committed_at LIMIT 1")
      .head().getLong(0)

    println("HISTORICAL ORDERS - BEFORE (the seeded rows, first snapshot of the orders table):")
    spark.sql(s"""
      SELECT order_id, quantity, unit_price, order_status, last_event_id, last_event_type
      FROM $Orders VERSION AS OF $firstSnapshot
      WHERE order_id IN $HistoricalOrders
      ORDER BY order_id
    """).show(truncate = false)

    println("HISTORICAL ORDERS - AFTER (current state, changed by ORDER_UPDATED / ORDER_CANCELLED):")
    spark.sql(s"""
      SELECT order_id, quantity, unit_price, order_status, last_event_id, last_event_type
      FROM $Orders
      WHERE order_id IN $HistoricalOrders
      ORDER BY order_id
    """).show(truncate = false)

    println("ORDERS - ALL:")
    spark.sql(s"SELECT * FROM $Orders ORDER BY order_id").show(100, truncate = false)

    // 7. Schema evolution: the new columns must exist in all four tables
    println("SCHEMA EVOLUTION - COLUMNS ADDED IN BATCH 5:")
    Seq(Bronze, Silver, Quarantine, Orders).foreach { table =>
      println(table)
      spark.sql(s"DESCRIBE TABLE $table")
        .filter("col_name IN ('sales_channel', 'discount_amount')")
        .show(truncate = false)
    }

    println("SILVER - ROWS THAT CARRY THE NEW COLUMNS:")
    spark.sql(s"""
      SELECT event_id, order_id, sales_channel, discount_amount, batch_id
      FROM $Silver
      WHERE sales_channel IS NOT NULL
      ORDER BY event_id
    """).show(100, truncate = false)

    // 8. Iceberg snapshots: one per micro-batch for bronze and silver
    Seq(Bronze, Silver, Quarantine, Orders, DuplicateAudit).foreach { table =>
      println(s"SNAPSHOTS OF $table:")
      spark.sql(s"""
        SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added_records
        FROM $table.snapshots
        ORDER BY committed_at
      """).show(truncate = false)
    }

    spark.stop()
  }
}