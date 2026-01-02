# 🐾 Salvando Patitas - Data Platform (SPDP)

**Production-Grade Cloud Native Data Pipeline** powered by **Apache Airflow**, **PySpark**, and **Google Cloud Platform**.

![Status](https://img.shields.io/badge/Status-Production-green)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-3.5-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.10-red)

---

## 🏗️ Architecture

This platform implements a **Modern Data Lakehouse** architecture using GCP Service-managed infrastructure.

```mermaid
graph LR
    SRC[Supabase (PostgreSQL)] -->|Extract Script| RAW[GCS Data Lake (Raw)]
    RAW -->|Spark (Clean/Dedupe)| SILVER[GCS Data Lake (Silver)]
    SILVER -->|Spark (Aggregate)| GOLD[GCS Data Lake (Gold)]
    GOLD -->|Load| BQ[BigQuery Data Warehouse]
    BQ -->|Connect| BI[Looker Studio]
```

### 🔹 Layers
1.  **Bronze (Raw):** Parquet files partitioned by `year/month` (or `year/month/day` for transactions). Hybrid extraction strategy (Full Load for Dimensions, Incremental for Facts).
2.  **Silver (Refined):** Cleaned data, standardized schemas, deduplication logic, and data quality checks (e.g., email validation, null handling).
3.  **Gold (Curated):** Business-level aggregates, dimensional models (Star Schema), and feature engineering (RFM, LTV).

---

## ⚡ Performance Optimization ("War Stories")

We achieved a **~700% Performance Boost** (30 mins → 4 mins) by optimizing for Cloud Object Storage.

1.  **GCS vs POSIX:** Spark's default `rename` commit protocol is expensive on GCS (Copy+Delete). We implemented a custom **conditional commit protocol** that writes directly to the final destination in Prod, bypassing the rename step.
2.  **Hybrid Loading:** To solve schema evolution and consistency issues, we implemented a **Hybrid Strategy**:
    -   **Dimensions (Donors, Cases):** Full Snapshot every run (ensures updates are captured).
    -   **Facts (Donations, Expenses):** Incremental Append (High volume efficiency).
3.  **Caching:** Strategic use of Spark `.cache()` for repeated DataFrame actions prevents re-computation.

---

## 🚀 Deployment & Operations

### Cloud Environment (Production)
-   **Infrastructure:** GCP Compute Engine (Ubuntu 22.04, 4 vCPU, 16GB RAM).
-   **Security:** Workload Identity (No JSON keys stored).
-   **Networking:** SSH Tunneling for Airflow UI access (No public IP exposure on port 8080).

### How to Run (VM)
1.  **Access Airflow:**
    ```bash
    # Run on your local machine to create tunnel
    gcloud compute ssh airflow-server-prod --zone=us-central1-a -- -L 8080:localhost:8080
    ```
    Go to: `http://localhost:8080`

2.  **Deploy Updates:**
    ```bash
    cd ~/data-plataform-fsp-spark-airflow
    git pull
    docker compose -f docker-compose.prod.yaml restart
    ```

3.  **Run Pipeline:**
    -   Trigger `spdp_data_platform_main` in Airflow UI.
    -   Logs are sent to Slack (`#data-alerts`) and Airflow UI.

---

## 🛠️ Project Structure

```bash
├── config/              # Environment-specific configs (Cloud vs Local)
├── dags/                # Airflow DAG definitions
├── jobs/                # PySpark Jobs
│   ├── silver/          # Cleaning Logic
│   ├── gold/            # Aggregation Logic
│   └── utils/           # Shared helpers (GCS, Spark Session, Alerts)
├── scripts/             # Python Scripts
│   ├── extract_from_supabase.py # The Extraction Engine
│   └── run_pipeline.sh          # Manual execution helper
└── docker-compose.prod.yaml # Production Orchestration
```

---

## 📊 Key Metrics
-   **Historical Data:** ~30,000 Total Records processed in < 35 seconds (Extraction).
-   **Pipeline Latency:** ~5 minutes End-to-End.
-   **Reliability:** Auto-retries, Dead Letter Queue (Quarantine) for bad data, Slack Alerting.

---

*Maintainers: Vladislav Marinovich*
