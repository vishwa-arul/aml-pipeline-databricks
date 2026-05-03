# End-to-End AML Transaction Monitoring Pipeline (PySpark + Databricks)

## Project Overview

This project implements a production-style Anti-Money Laundering (AML) data pipeline using PySpark and Delta Lake on Databricks.

The pipeline processes transaction data, applies data cleaning, enrichment, and AML rule-based detection, and generates alerts such as:

* LCTR (Large Cash Transaction Report)
* STR (Suspicious Transaction Report)

It follows the Medallion Architecture (Bronze → Silver → Gold) and supports both batch and streaming ingestion.

---

## Architecture

```
Source (CSV / Kafka / Confluent)
        ↓
Bronze Layer (Raw - Delta)
        ↓
Silver Layer (Cleaned + Deduplicated)
        ↓
Enrichment Layer (Risk Data Join)
        ↓
Gold Layer (AML Rules: LCTR, STR)
        ↓
Serving Layer (Dashboard - Power BI / Databricks SQL)
```

---

## Technologies Used

* PySpark (DataFrame API)
* Delta Lake
* Databricks
* Structured Streaming
* Kafka / Confluent (simulated/used)
* Databricks Workflows (Orchestration)
* SQL (Dashboard Queries)
* Power BI / Databricks SQL Dashboard

---

## Project Structure

```
aml-pipeline-project/
│
├── notebooks/
│   ├── bronze/        # Ingestion
│   ├── silver/        # Cleaning & Deduplication xand  Data Enrichment
│   ├── gold/          # AML Rules (LCTR, STR)
│   
│
├── sql/
│   └── dashboard_queries.sql
│
│
├── screenshots/       # Dashboard & Workflow Proof
│
└── README.md
```

---

## Data Pipeline Flow

### Bronze Layer (Raw Ingestion)

* Ingest transaction data from CSV / Kafka
* Add metadata:

  * ingestion_time
  * source_file
* Store in Delta format

---

### Silver Layer (Cleaning & Deduplication)

* Remove null values
* Data type validation
* Deduplicate using:

  * `dropDuplicates()` (batch)
  * `withWatermark()` (streaming)
* Standardize fields (country, timestamps)

---

### Enrichment Layer

Join external datasets:

* High-risk countries
* Watchlist (terror/sanctions)
* Customer risk profile

Derived flags:

* `is_high_risk_country`
* `is_watchlist_hit`

---

### Gold Layer (AML Rule Engine)

#### LCTR Rules

* Transaction amount > 1,00,000
* Aggregated daily amount > threshold

#### STR Rules

* High transaction velocity
* Structuring (multiple near-threshold transactions)
* High-risk country transactions
* Watchlist matches
* Geographic anomalies

---

## Serving Layer (Dashboard)

AML alerts are visualized using:

* Power BI / Databricks SQL Dashboard

### Key Insights:

* Suspicious transactions trend
* Top risky customers
* Country-wise risk distribution
* Rule-wise alert breakdown

---

## Streaming Support

* Kafka / Confluent used for real-time ingestion
* Structured Streaming pipeline:

  * Bronze → Silver (real-time)
* Checkpointing for fault tolerance

---

## Performance Optimizations

* Delta Lake storage
* Partitioning (txn_date)
* Z-Ordering for faster queries
* Caching (where applicable)

---



---

## Screenshots

### Dashboard

### Workflow

### Delta Tables

---

## How to Run

1. Import notebooks into Databricks
2. Update paths to respective code
3. Run pipeline in order:

   * Bronze → Silver → Enrich → Gold
4. (Optional) Start streaming jobs
5. View results in dashboard

---

## Key Highlights

* End-to-end data pipeline (batch + streaming)
* Real-world AML use case
* Medallion architecture implementation
* Rule-based fraud detection system
* Dashboard for business insights

---

## GitHub Repository

https://github.com/vishwa-arul/aml-pipeline-databricks

---

## Future Enhancements

* ML-based fraud detection (PySpark ML / MLflow)
* Real-time alert system (Kafka → API)
* Advanced orchestration (Airflow)
* Data quality framework integration

---

## Author

Vishwa Arul

---

## If you found this useful, give it a star!
