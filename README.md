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
1.  **Bronze (Raw):** Parquet files partitioned by `year/month/day` preservando granularidad diaria para auditoría. Hybrid extraction strategy (Full Load for Dimensions, Incremental for Facts).
2.  **Silver (Refined):** 
    - **Particionamiento Mensual (`y/m`)**: Optimizado para Cloud Storage (GCS), reduciendo overhead de listado de archivos.
    - **Nombres Nativos de Spark**: Archivos mantienen formato `part-*.snappy.parquet` (sin renombrado manual) para máxima velocidad.
    - **Modos de Escritura**:
      - Facts (Donaciones, Gastos): `append` mode - acumulación de archivos diarios dentro de partición mensual.
      - Dimensions (Donantes, Casos): `overwrite` mode - snapshot mensual limpia.
    - Data Quality: Deduplicación, validación de esquemas, cuarentena para registros inválidos.
3.  **Gold (Curated):** Business-level aggregates, dimensional models (Star Schema), y feature engineering (RFM, LTV).

---

## ⚡ Performance Optimization (Enero 2026)

### Objetivo: Estabilidad + Velocidad en VM de 16GB RAM

**Problema Original:**
- OOM Killer matando procesos Spark (Concurrencia ilimitada saturaba memoria)
- Escrituras lentas en GCS por "Small File Problem" (particionamiento diario)
- Latencia de 30+ minutos en jobs Silver

**Soluciones Implementadas:**

1.  **Control de Memoria Estricto:**
    ```yaml
    AIRFLOW__CORE__PARALLELISM=2  # Max 2 jobs concurrentes
    SPARK_DRIVER_MEMORY=2g
    SPARK_EXECUTOR_MEMORY=4g
    # Total: ~12GB usados, dejando 4GB al SO
    ```

2.  **Particionamiento Mensual (`y/m`):**
    - **Antes**: Partición diaria (`y/m/d`) → Miles de archivos pequeños → Overhead masivo en GCS.
    - **Ahora**: Partición mensual → Archivos se acumulan en carpetas mensuales → ~30x más rápido.

3.  **Nombres Nativos de Spark:**
    - **Antes**: Renombrado manual (`part-0000.parquet`) → Copy+Delete en GCS (lento + race conditions).
    - **Ahora**: Nombres hash (`part-abc123.snappy.parquet`) → Escritura directa sin renombrado.

**Resultado:**
- ✅ **Estabilidad**: 99.9% (sin OOM).
- ✅ **Velocidad**: `silver_donaciones` 30min → 3min (~90% reducción).
- ✅ **Escalabilidad**: Arquitectura preparada para 10x volumen de datos.

### 📊 Guía de Tuning (Según Volumen de Datos)

| Volumen de Datos | Parallelism | Executor Memory | RAM VM | Tiempo Pipeline |
|------------------|-------------|-----------------|--------|-----------------|
| ~30k registros (actual) | 2 | 4g | 16GB | ~10-15 min |
| ~300k (10x) | 3 | 3g | 16GB | ~7-10 min |
| ~3M (100x) | 4 | 4g | 32GB | ~10-15 min |

**⚠️ Nota**: Siempre dejar 4GB+ libres para el Sistema Operativo.

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
-   **Pipeline Latency:** ~10-15 minutes End-to-End (Optimized from 30+ min).
-   **Reliability:** Auto-retries, Dead Letter Queue (Quarantine) for bad data, Slack Alerting.

---

*Maintainers: Vladislav Marinovich*
