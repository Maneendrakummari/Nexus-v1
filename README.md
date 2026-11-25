# Project Nexus – End-to-End Data Engineering Project

## 📌 Project Overview

Project Nexus is an end-to-end data engineering project that processes e-commerce data using both **batch** and **streaming** pipelines.

- The **batch pipeline** uses Scala, Apache Spark, Azure Databricks, Azure Data Lake Storage (ADLS Gen2), and Delta Lake.
- The **streaming pipeline** uses Scala, Apache Kafka, Spark Structured Streaming, and Apache Iceberg, and is deployable on Kubernetes.

The batch pipeline implements the Medallion Architecture (Bronze, Silver, and Gold) to ingest raw data, perform data cleansing and validation, process incremental updates, and transform the data into analytics-ready datasets. The final analytical data model follows a Star Schema, making the processed data suitable for business intelligence and reporting using Power BI.

The streaming pipeline ingests order events from Kafka in micro-batches, keeps every record as received in a Bronze table, separates valid and invalid events, handles duplicates and late data, evolves the schema, and maintains the current state of each order using Iceberg `MERGE INTO`.

---

## 🎯 Project Objectives

**Batch pipeline**

- Build an end-to-end batch data processing pipeline.
- Ingest multiple CSV datasets from Azure Data Lake Storage.
- Implement the Medallion Architecture.
- Perform data cleansing, standardization, and validation.
- Handle null values, duplicate records, and invalid data.
- Separate valid and rejected records.
- Implement incremental processing using Delta Lake MERGE.
- Identify and ignore stale updates using timestamps.
- Handle schema evolution when new columns are introduced.
- Maintain data consistency and prevent duplicate processing.
- Design a Star Schema using fact and dimension tables.
- Perform data quality checks across all layers.
- Prepare analytical datasets for Power BI dashboards.

**Streaming pipeline**

- Stream order events from Kafka through Spark Structured Streaming into Apache Iceberg tables.
- Process events in five micro-batches that produce five Iceberg snapshots.
- Preserve every received record in a Bronze table.
- Validate events and store valid, de-duplicated events in a Silver table.
- Quarantine invalid events with one or more reject reasons per record.
- Detect and resolve duplicate events within and across batches, with an audit trail.
- Apply a 5-minute watermark policy to identify late events.
- Handle schema evolution when new columns arrive mid-stream.
- Maintain current order state (create, update, cancel) using `MERGE INTO`.
- Deploy the pipeline on Kubernetes.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Scala | Primary programming language (batch and streaming) |
| Apache Spark | Distributed data processing |
| Spark Structured Streaming | Micro-batch stream processing (`foreachBatch`) |
| Azure Databricks | Batch data engineering and processing platform |
| Azure Data Lake Storage Gen2 | Cloud data storage for the batch pipeline |
| Delta Lake | Reliable storage, MERGE, and incremental processing (batch) |
| Unity Catalog | Data and table organization (batch) |
| Apache Kafka | Event streaming source (topic `retail`) |
| Apache Iceberg | Table format for the streaming pipeline (`MERGE INTO`, snapshots, schema evolution) |
| Docker | Local Kafka and container image for the pipeline |
| Kubernetes | Deployment of Kafka, the Spark streaming job, and the producer |
| sbt | Scala build tool |
| Git & GitHub | Source code management and version control |

**Streaming component versions**

| Component | Version |
|---|---|
| Scala | 2.12.18 |
| sbt | 1.10.7 |
| Spark | 3.5.9 |
| Iceberg (`iceberg-spark-runtime-3.5_2.12`) | 1.12.0 |
| Java | 17 |
| Kafka | Docker (local); `apache/kafka` 3.9.1 KRaft (Kubernetes) |
| Catalog | Hadoop catalog named `local` |

---

## 📂 Dataset Overview

The project uses synthetic e-commerce datasets.

**Batch datasets**

| Dataset | Description |
|---|---|
| `products.csv` | Product information |
| `customers.csv` | Customer information |
| `orders.csv` | Initial order transactions |
| `orders_updates.csv` | Incremental order updates |
| `orders_v2.csv` | Updated order schema with additional columns |
| `data_dictionary.csv` | Dataset fields and validation rules |

**Streaming datasets**

| Dataset | Description |
|---|---|
| `order_events.jsonl` | 62 order events, arriving in `ingest_time` order between 10:00 and 11:00 UTC on 19 Nov 2026 |
| `micro_batches/order_events_001..005.json` | The events split into five micro-batch files of 13, 13, 13, 13, and 10 events (JSON lines, one event per line) |
| `order_events_kafka_keyed.txt` | Keyed version of the events for Kafka |
| `order_event.schema.json` | JSON schema of an order event |

`ingest_time` is an added field used for arrival order, de-duplication order, and the late-data policy.

**Built-in data quality issues in the streaming data**

| Issue | Events |
|---|---|
| Duplicate `event_id` (5) | EVT000029, EVT000031, EVT000033, EVT000034, EVT000051 |
| Late events (21 to 25 minutes after `event_time`) | EVT000055, EVT000056, EVT000057 |
| Null `order_id` | EVT000048 |
| Invalid event type `ORDER_ERR` | EVT000049 |
| Negative quantity | EVT000050 |
| Negative price | EVT000051 (both copies) |
| Orphan customer `CUST999` | EVT000052 |
| Orphan product `PRD888` | EVT000053 |
| Future `event_time` (clock skew) | EVT000054 |

Three events (EVT000045, EVT000046, EVT000047) update or cancel historical orders ORD000011, ORD000041, and ORD000066 and are used to demonstrate `MERGE INTO`.

---

## 🏗️ Data Architecture

The project has two pipelines. The batch pipeline follows the Medallion Architecture on Delta Lake. The streaming pipeline follows a Bronze, Silver, and Quarantine pattern on Apache Iceberg.

### Part A – Batch Pipeline

#### 1. Bronze Layer – Raw Data Ingestion

The Bronze layer is responsible for ingesting source CSV datasets from ADLS Gen2 and storing them as Delta tables.

Implemented features:

- Read CSV files from ADLS Gen2 using Apache Spark.
- Preserve source data in its raw form.
- Use header-based CSV ingestion.
- Initially ingest columns as strings to retain source values.
- Add ingestion metadata:
  - `_ingestion_timestamp`
  - `_source_file`
  - `_batch_id`
- Store ingested data in Delta format.
- Register Bronze tables using Unity Catalog.

Bronze tables:

- `retail.bronze.customers`
- `retail.bronze.products`
- `retail.bronze.orders`
- `retail.bronze.orders_updates`
- `retail.bronze.orders_v2`

#### 2. Silver Layer – Data Cleansing and Validation

The Silver layer transforms raw Bronze data into clean, standardized, and validated datasets.

Implemented features:

- Handle null and missing values.
- Remove exact duplicate records.
- Identify business-key duplicates.
- Retain the latest record using `last_updated` and window functions.
- Convert columns into appropriate data types.
- Handle invalid type conversions.
- Standardize text fields using trimming and case transformations.
- Validate email formats using regular expressions.
- Apply business validation rules.
- Validate primary keys and foreign key relationships.
- Separate valid and rejected records.
- Maintain rejection reasons for invalid records.
- Retain inactive products where required by the business logic.

Silver tables:

- `retail.silver.customers`
- `retail.silver.products`
- `retail.silver.orders`
- `retail.silver.rejected_products`

#### 3. Incremental Data Processing

The project implements incremental order processing using Delta Lake MERGE.

The `orders_updates.csv` dataset contains new and updated order records.

Implemented features:

- Identify new orders and existing orders.
- Process incoming updates.
- Deduplicate incoming records using window functions.
- Select the latest incoming version based on `last_updated`.
- Ignore stale updates when the target contains a newer version.
- Validate incoming records before merging.
- Apply updates and inserts using Delta Lake MERGE.
- Prevent duplicate processing through appropriate merge conditions.
- Verify repeated MERGE execution for idempotent behavior.
- Inspect Delta transaction history and use time travel for data verification.

#### 4. Schema Evolution

The project handles changes in the incoming order schema using the `orders_v2.csv` dataset.

New columns introduced include:

- `sales_channel`
- `discount_amount`

Implemented features:

- Identify newly introduced columns.
- Add the new columns to the target schema.
- Process records containing the updated schema.
- Apply incremental MERGE operations.
- Preserve existing records.
- Allow older records to contain NULL values for newly introduced columns.

#### 5. Gold Layer – Analytical Data Modelling

The Gold layer transforms Silver data into analytics-ready datasets.

The project uses dimensional modelling and a Star Schema.

Implemented features:

- Create dimension tables.
- Create a fact table.
- Generate and maintain dimension surrogate keys.
- Connect fact records to dimensions using foreign keys.
- Validate dimension keys and referential integrity.
- Perform data quality checks.
- Reconcile Silver and Gold record counts based on the project's fact-table grain.

### Part B – Streaming Pipeline (Kafka → Spark → Iceberg)

Each micro-batch travels from a file through Kafka into one Spark `foreachBatch` run, which writes the Iceberg tables.

```
order_events_00N.json ──► Producer ──► Kafka topic "retail" ──► Spark Structured Streaming
                                                                     (foreachBatch)
                                                                          │
                          ┌───────────────┬───────────────┬──────────────┼───────────────┐
                          ▼               ▼               ▼              ▼               ▼
                       bronze          silver        quarantine       orders      duplicate_audit
```

How it works:

- The producer publishes one micro-batch file to topic `retail` every 20 seconds, longer than the 10-second trigger, so one file becomes one micro-batch and five files give five Iceberg snapshots.
- The Kafka key is `order_id`, or `event_id` when `order_id` is null.
- The stream reads from the earliest offset, and its checkpoint stores the Kafka offsets so a restart resumes where it stopped.
- Batch ids run from 1 to 5; the empty startup batch 0 is skipped.

#### Iceberg Tables

All tables live in namespace `local.orders` of the Hadoop catalog `local`.

| Table | Holds | Written by |
|---|---|---|
| `bronze_order_events` | Every record as received: Kafka partition, offset and key, `raw_json`, parsed fields, `event_ts`, `ingest_ts`, `batch_id` | Append per batch |
| `silver_order_events` | Valid, de-duplicated events | `MERGE INTO` by `event_id` (a newer `ingest_ts` copy updates the row) |
| `quarantine_order_events` | Rejected records with `reject_reasons` and the `watermark_ts` applied | Append per batch |
| `orders` | Current state per order | `MERGE INTO` from events (create, update, cancel) |
| `duplicate_audit` | One row per duplicated `event_id` with its resolution | Append per batch |
| `dim_customer` | Customer IDs CUST001 to CUST040, used for orphan checks | Seeded at start |
| `dim_product` | Product IDs PRD001 to PRD024, used for orphan checks | Seeded at start |

The `orders` table is also seeded with three historical orders (ORD000011, ORD000041, ORD000066) so the update and cancel events have something to change.

#### Validation and Reject Reasons

Each record is checked against all rules at once, so a quarantined record carries every reason that applies.

| Reason | Rule |
|---|---|
| `MALFORMED_RECORD` | JSON could not be parsed |
| `INVALID_EVENT_ID_FORMAT` | `event_id` does not match `^EVT\d{6}$` |
| `INVALID_ORDER_ID_FORMAT` | `order_id` does not match `^ORD\d{6}$` |
| `INVALID_CUSTOMER_ID_FORMAT` | `customer_id` does not match `^CUST\d{3}$` |
| `INVALID_PRODUCT_ID_FORMAT` | `product_id` does not match `^PRD\d{3}$` |
| `NULL_ORDER_ID` | `order_id` is null |
| `INVALID_EVENT_TYPE` | Type is not a valid event type (for example `ORDER_ERR`) |
| `INVALID_QUANTITY` | `quantity` is 0 or less |
| `INVALID_PRICE` | `unit_price` is 0 or less |
| `INVALID_DISCOUNT` | `discount_amount` is below 0 |
| `ORPHAN_CUSTOMER` | Customer not in `dim_customer` |
| `ORPHAN_PRODUCT` | Product not in `dim_product` |
| `MISSING_REQUIRED_FIELD` | A required field is null |
| `INVALID_TIMESTAMP` | `event_time` or `ingest_time` cannot be parsed |
| `FUTURE_EVENT_TIME` | `event_time` is after the event's own `ingest_time` |
| `LATE_EVENT` | `event_ts` is earlier than the watermark |
| `DUPLICATE_EVENT` | A superseded copy of an `event_id` |
| `UNKNOWN_ORDER` | Update or cancel for an order that does not exist |

#### 5-Minute Watermark and Late Data

- For each micro-batch, the watermark is the latest `event_ts` seen in earlier batches (ignoring events dated after their own `ingest_ts`) minus 5 minutes.
- It is read from Bronze before the current batch is appended.
- An event with `event_ts` earlier than the watermark is a `LATE_EVENT` and goes to quarantine; arrivals within 5 minutes are accepted.
- The watermark is computed per batch from Bronze rather than with Spark's `withWatermark`, so Bronze stays a raw copy of what arrived.
- A simple "ingest lag over 5 minutes" rule was rejected because it would wrongly flag valid duplicates such as EVT000051 and EVT000031.

#### De-duplication

- Within a batch, copies of one `event_id` are ranked by `ingest_ts` and then Kafka offset, and only the latest continues.
- Across batches, Silver is upserted with `MERGE INTO`: a copy with a newer `ingest_ts` updates the row, otherwise it is ignored.
- Each case is recorded in `duplicate_audit`:

| Resolution | Meaning |
|---|---|
| `SUPERSEDED_IN_BATCH` | Several copies in one batch; the latest by `ingest_ts` and Kafka offset won |
| `DUPLICATE_COPIES_REJECTED` | Copies of an event that is itself invalid |
| `MERGED_INTO_SILVER` | A later batch re-delivered an event already in Silver; the newer copy updated it |
| `IGNORED_OLDER_COPY` | A re-delivered copy older than the stored one, ignored |

#### Order State with MERGE INTO

- `ORDER_CREATED` events are inserted; an existing order is left alone.
- `ORDER_CANCELLED` sets the status to `CANCELLED` and records the last event.
- `ORDER_UPDATED` changes customer, product, quantity, and price where provided and sets the status to `UPDATED`.
- An update applies only if the order is not already `CANCELLED` and the event is not older than the order's last event.
- An update or cancel for an unknown order is quarantined as `UNKNOWN_ORDER`.

#### Streaming Schema Evolution

Batch 5 introduces two new fields, `sales_channel` (STRING) and `discount_amount` (DOUBLE).

- When the job sees either name in a batch's raw JSON and the table lacks the column, it runs `ALTER TABLE ... ADD COLUMN` on Bronze, Silver, quarantine, and `orders` before writing.
- Rows from earlier batches keep NULL in the new columns.
- Incoming batches are aligned to each table's current columns, with NULL for any that are missing.

#### Kubernetes Deployment

The same code is packaged to run on Kubernetes (Docker Desktop cluster) in namespace `orders`.

- Spark runs in cluster mode through a submit Job, which starts one driver pod and one executor pod.
- Kafka runs as a single KRaft pod.
- One shared volume (PVC `iceberg-data`, 2Gi) holds the Iceberg warehouse and checkpoints.
- The image `orders-pipeline:1.0` is built from `debian:bookworm-slim` with OpenJDK 17, Spark 3.5.9, the Kafka and Iceberg jars, the application jar, and the data folder.

| Manifest | Creates |
|---|---|
| `01-base.yaml` | Namespace, PVC, ServiceAccount with Role and RoleBinding |
| `02-kafka.yaml` | Kafka KRaft Deployment, Service `kafka:9092`, Job to create topic `retail` |
| `03-streaming-submit-job.yaml` | Job running `spark-submit` against the cluster |
| `04-producer-job.yaml` | Producer Job |
| `05-read-tables-job.yaml` | Verification Job that prints the tables |

`deploy.ps1` applies the manifests in order and `cleanup.ps1` removes the namespace.

---

## ⭐ Star Schema

The analytical model for the batch pipeline consists of one fact table and three dimension tables.

**Fact Table**

- `Fact_Order`

Contains order-related measures and foreign keys connecting the fact records to the dimension tables.

**Dimension Tables**

- `Dim_Customer`
- `Dim_Product`
- `Dim_Date`

**Grain:** One row represents one order for one product (order-product combination).

The Star Schema supports analytical queries and business reporting across customers, products, dates, and order transactions.

---

## 📊 Business Questions

The analytical datasets are designed to answer the following business questions:

1. What is the monthly order trend?
2. Which are the top 10 most ordered products?
3. Which product categories have the highest order volume?
4. Who are the top 10 customers based on the number of orders?
5. What is the order status distribution (Placed, Shipped, Delivered, Cancelled, Returned)?

---

## 🧪 Data Quality and Testing

Testing is performed using Scala and Apache Spark DataFrame operations.

### Bronze Layer Testing (Batch)

- Verify Bronze table existence.
- Validate ingestion metadata columns.
- Check for NULL metadata values.
- Verify that tables contain records.

### Silver Layer Testing (Batch)

- Check for NULL primary keys.
- Identify duplicate business keys.
- Validate product names and categories.
- Verify positive product prices.
- Validate email formats.
- Check customer and product foreign key references.
- Verify rejected records and rejection reasons.

### Incremental Processing Testing (Batch)

- Verify order-key uniqueness after incremental processing.
- Inspect incoming updates against existing target records.
- Identify stale update candidates.
- Validate incremental processing results.

### Gold Layer Testing (Batch)

- Check dimension surrogate keys for NULL values.
- Identify duplicate dimension keys.
- Validate fact-to-dimension relationships.
- Verify referential integrity.
- Reconcile Gold and Silver record counts based on the defined grain.

### Streaming Pipeline Verification

The `ReadOrderTables` job verifies the streaming results after a run:

- Row counts for Bronze, Silver, quarantine, `orders`, and `duplicate_audit`.
- Quarantine records grouped by reject reason.
- Duplicate audit resolutions per `event_id`.
- Before and after state of the historical orders changed by `MERGE INTO`.
- Schema evolution check using `DESCRIBE TABLE` and the Silver rows that carry the new columns.
- Iceberg snapshot history per table.

**Verified results of a clean local run**

| Table | Rows | Detail |
|---|---|---|
| Bronze | 62 | Batches of 13, 13, 13, 13, 10 |
| Silver | 47 | 47 distinct `event_id` |
| Quarantine | 13 | LATE_EVENT 3, DUPLICATE_EVENT 2, INVALID_PRICE 2, NULL_ORDER_ID 1, INVALID_EVENT_TYPE 1, INVALID_QUANTITY 1, ORPHAN_CUSTOMER 1, ORPHAN_PRODUCT 1, FUTURE_EVENT_TIME 1 |
| Orders | 47 | 44 CREATED, 2 UPDATED, 1 CANCELLED |
| Duplicate audit | 5 | EVT000029 and 031 SUPERSEDED_IN_BATCH; EVT000051 DUPLICATE_COPIES_REJECTED; EVT000033 and 034 MERGED_INTO_SILVER |

**Historical orders after MERGE INTO**

| Order | Before | After |
|---|---|---|
| ORD000011 | quantity 2, CREATED | quantity 3, UPDATED |
| ORD000041 | quantity 4, CREATED | CANCELLED |
| ORD000066 | quantity 2, CREATED | quantity 4, UPDATED |

**Iceberg snapshots:** Bronze has 5 appends; Silver has 4 appends and 1 overwrite (the MERGE in batch 5); quarantine has 2 (batches 3 and 4).

---

## 📈 Expected Outcome

The project demonstrates end-to-end batch and streaming data engineering pipelines that transform raw e-commerce data into reliable, validated, and analytics-ready datasets.

It demonstrates practical implementation of:

- Cloud data ingestion.
- Distributed processing with Apache Spark.
- Medallion Architecture.
- Data quality and validation.
- Incremental data processing.
- Delta Lake MERGE operations.
- Stale update handling.
- Schema evolution.
- Dimensional modelling.
- Star Schema design.
- Data quality testing.
- Event streaming with Apache Kafka.
- Spark Structured Streaming with `foreachBatch`.
- Apache Iceberg tables, snapshots, and `MERGE INTO`.
- Late-data handling with a watermark policy.
- Duplicate event detection with an audit trail.
- Quarantine of invalid events with reject reasons.
- Containerized deployment on Kubernetes.

The final Gold datasets are intended to support business intelligence and visualization through Power BI.

---

## ⚠️ Streaming Assumptions and Limitations

**Assumptions**

- No master data was supplied, so `dim_customer` (CUST001 to CUST040) and `dim_product` (PRD001 to PRD024) are assumed ranges that cover every valid ID in the data.
- The three historical orders and their starting quantities are seed values.
- The watermark is computed per micro-batch from Bronze, not with Spark's `withWatermark` and window aggregation.
- The Kubernetes container runs as root so pods can write to the shared volume; this is a demo simplification.

**Limitations**

- Late events older than the watermark are rejected; there is no reprocessing path for them.
- When a batch has no order changes, the second `orders` MERGE still runs and creates an empty overwrite snapshot. Skipping it when the change count is 0 is a possible improvement.
- The warehouse and checkpoint are local files (a shared volume on Kubernetes), not object storage.
- The streaming pipeline currently runs separately from the batch pipeline: it uses Iceberg with a local Hadoop catalog and its own seeded dimensions, and does not yet feed the Delta Lake Gold Star Schema.

---

## 🚧 Project Status

Currently in development.

- **Batch pipeline:** the core pipeline, Bronze and Silver transformations, incremental processing, schema evolution, and Gold dimensional modelling are being implemented and validated.
- **Streaming pipeline:** the Kafka → Spark → Iceberg pipeline is implemented and verified in a clean local run on Windows (results above). The Docker image build is confirmed. The Kubernetes manifests and `deploy.ps1` are provided, but a full deployment run has not yet been confirmed, so a first deployment should be treated as a test against the expected results above.
- Power BI dashboard development and additional enhancements are planned as the next steps.

---

## 🔮 Future Enhancements

- Develop interactive Power BI dashboards.
- Confirm and validate the full Kubernetes deployment end to end.
- Move the streaming warehouse and checkpoints to object storage.
- Add a reprocessing path for late events.
- Skip empty `orders` MERGE runs when a batch has no order changes.
- Integrate streaming data with the Gold layer and Star Schema.
- Replace seeded streaming dimensions with the real customer and product master data.
- Run Kafka and the streaming job continuously with real-time order events instead of file-driven micro-batches.
- Automate batch and streaming pipeline execution and scheduling.
- Add monitoring, logging, and failure notifications.
- Implement automated data quality checks.
- Improve pipeline performance through Spark optimization techniques.

---

**Project:** Nexus – End-to-End Data Engineering Pipeline
**Architecture:** Medallion Architecture + Star Schema (batch), Kafka → Spark Structured Streaming → Iceberg (streaming)
**Processing Type:** Batch and Streaming Data Processing
**Primary Language:** Scala
