# 🐾 Salvando Patitas - Data Platform (SPDP) 🚀

## 1. Executive Summary
**Salvando Patitas Data Platform (SPDP)** is a production-grade, cloud-native data pipeline designed to ingest, process, and analyze rescue animal data. 
- **Goal:** Enable data-driven decisions for animal rescue operations (Donations, Expenses, Cases, Shelters).
- **Tech Stack:** Airflow, PySpark, Google Cloud Platform (Compute Engine, GCS, BigQuery), Docker, Supabase (PostgreSQL).
- **Status:** **Production Ready** 🟢.
- **Performance:** End-to-End pipeline runtime: **~4-5 minutes** (Processing ~30k+ records).

---

## 2. Architecture Overview 🏗️

### High-Level Diagram
```mermaid
graph LR
    SUB[Supabase DB] -->|Extract (Pandas)| RAW[GCS Data Lake (Raw)]
    RAW -->|Spark Process| SILVER[GCS Data Lake (Silver)]
    SILVER -->|Spark Aggregates| GOLD[GCS Data Lake (Gold)]
    GOLD -->|Load Job| BQ[Google BigQuery]
    BQ -->|Connect| LOOKER[Looker Studio]
    
    airflow((Airflow)) -.->|Orchestrates| SUB
    airflow -.->|Orchestrates| RAW
    airflow -.->|Orchestrates| SILVER
    airflow -.->|Orchestrates| GOLD
    airflow -.->|Orchestrates| BQ
```

### Components
1.  **Ingestion Layer (Bronze/Raw)**:
    -   Script: `extract_from_supabase.py`
    -   Logic: Hybrid Loading.
        -   **Incremental Tables** (Donations, Expenses): Fetches only new data based on `last_modified_at`.
        -   **Full Load Tables** (Donors, Cases, Shelters): Fetches complete history to ensure dimensional consistency.
    -   Storage: Google Cloud Storage (Bucket `gs://salvando-patitas-spark/lake/raw`).
    -   Format: Parquet (Hive-partitioned).

2.  **Processing Layer (Silver)**:
    -   Engine: Apache Spark 3.5 (PySpark).
    -   Logic: Cleaning, Deduplication, Standardization (e.g., Default Country='Colombia', Date Parsing).
    -   Optimization: Use of `.cache()` for heavy DataFrames.

3.  **Analytics Layer (Gold)**:
    -   Engine: Apache Spark 3.5 (PySpark).
    -   Ouputs:
        -   **Dimensions**: `dim_donantes`, `dim_casos`, `dim_calendario`.
        -   **Facts**: `fact_donaciones`, `fact_gastos`.
        -   **Aggregated Features**: RFM Analysis, Donor LTV.
        -   **Dashboards**: Pre-aggregated tables for BI speed.

4.  **Serving Layer**:
    -   Destination: Google BigQuery.
    -   Dataset: `salvando-patitas-de-spark.gold`.

---

## 3. Key Challenges & Solutions ("War Stories") ⚔️

### 🚀 Performance Optimization: The 30-Minute to 5-Minute Win
**Problem:** The task `silver_donantes` was taking 30+ minutes despite processing only ~10k records. CPU usage was low.
**Diagnosis:** Spark's default behavior on File Systems (HDFS) is to write to temporary files and then "rename" (move) them to the final directory. On **Object Storage (GCS/S3)**, a "rename" is actually a **Copy + Delete** operation, which is extremely slow for many small files.
**Solution:**
-   Implemented a custom `file_utils.py` module.
-   **Disabled** the `rename_spark_output` function when running in `ENV=cloud` context.
-   Directly relied on Spark's Output Committer suitable for Cloud.
-   **Result:** Runtime dropped from **32 mins** to **4 mins**.

### 🧠 Hybrid Loading Strategy
**Problem:** `Donantes` (Donors) table had schema conflicts when running incrementally because updates to existing donors weren't reflecting correctly in the historical partitions.
**Solution:** Moved Dimensional Tables (`Donantes`, `Casos`, `Proveedores`) to a **Full Load** strategy. Every run fetches the latest Snapshot of these tables, ensuring 100% consistency. Fact tables remain Incremental for efficiency.

### 🛡️ Security: Workload Identity
**Problem:** Managing JSON Key Files for GCP Service Accounts inside Docker is risky and cumbersome.
**Solution:** Configured the VM with a User-Managed Service Account attached directly to the Compute Instance. The Airflow Docker container inherits these credentials via **Application Default Credentials (ADC)**. No keys stored in code!

---

## 4. Operational Runbook 📘

### How to Access Airflow
**Method:** SSH Tunnel (Most Secure).
1.  Run in local terminal:
    ```bash
    gcloud compute ssh airflow-server-prod --zone=us-central1-a -- -L 8080:localhost:8080
    ```
2.  Open browser: `http://localhost:8080`

### Pipeline Monitoring
-   **Slack Alerts:** 
    -   Success/Failure notifications sent to `#data-alerts` channel.
    -   Custom Bot Name: "SPDP Data Platform | Airflow".
-   **Logs:**
    -   Viewable directly in Airflow UI.
    -   Stored in `/opt/airflow/logs` inside the container.

### Disaster Recovery
-   **Re-run History:** Delete the `watermarks` folder in GCS. The pipeline will automatically detect a "Clean Slate" and fetch all history.
-   **Code Rollback:** 
    ```bash
    git reset --hard <commit_hash>
    docker compose restart
    ```

---

## 5. Metrics & Impact 📊
-   **Historical Data Loaded:** 
    -   Donations: 11,400+
    -   Donors: 9,200+
    -   Expenses: 7,000+
-   **Cost Efficiency:**
    -   VM shuts down automatically after pipeline success (if configured).
    -   Uses Spot Instances (optional) or low-cost E2 machines.

---

> *Documentation generated by Antigravity & Vladislav Marinovich - Jan 2026*
