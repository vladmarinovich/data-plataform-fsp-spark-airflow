# 🐾 Salvando Patitas - Data Platform (SPDP)

**Pipeline de Datos Cloud-Native en Producción** potenciado por **Apache Airflow**, **PySpark** y **Google Cloud Platform**.

![Status](https://img.shields.io/badge/Status-Production-green)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Spark](https://img.shields.io/badge/Spark-3.5-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.10-red)

---

## 🏗️ Arquitectura

Esta plataforma implementa una arquitectura moderna de **Data Lakehouse** utilizando infraestructura gestionada en GCP.

```mermaid
graph LR
    SRC[Supabase PostgreSQL] -->|Script de Extracción| RAW[GCS Data Lake - Raw]
    RAW -->|Spark Limpieza| SILVER[GCS Data Lake - Silver]
    SILVER -->|Spark Agregación| GOLD[GCS Data Lake - Gold]
    GOLD -->|Carga| BQ[BigQuery DW]
    BQ -->|Conexión| BI[Looker Studio]
```

### 🔹 Capas
1.  **Bronze (Raw):** Archivos Parquet particionados por `año/mes/día`, preservando granularidad diaria para auditoría. Estrategia de extracción híbrida (Carga Completa para Dimensiones, Incremental para Hechos).
2.  **Silver (Refinada):**
    - **Particionamiento Mensual (`y/m`)**: Optimizado para Cloud Storage (GCS), reduciendo el overhead de listado de archivos.
    - **Nombres Nativos de Spark**: Archivos mantienen formato `part-*.snappy.parquet` (sin renombrado manual) para máxima velocidad.
    - **Modos de Escritura**:
      - Facts (Donaciones, Gastos): Modo `append` - acumulación de archivos diarios dentro de partición mensual.
      - Dimensions (Donantes, Casos): Modo `overwrite` - snapshot mensual limpio.
    - **Calidad de Datos**: Deduplicación, validación de esquemas, cuarentena para registros inválidos.
3.  **Gold (Curada):** Agregados a nivel de negocio, modelos dimensionales (Esquema Estrella) e ingeniería de características (RFM, LTV).

---

## ⚡ Optimización de Rendimiento (Enero 2026)

### Objetivo: Estabilidad + Velocidad en VM de 16GB RAM

**Problema Original:**
- OOM Killer matando procesos Spark (Concurrencia ilimitada saturaba la memoria).
- Escrituras lentas en GCS por "Small File Problem" (particionamiento diario).
- Latencia de 30+ minutos en jobs Silver y fallos constantes.

**Soluciones Implementadas:**

1.  **Control de Memoria Estricto:**
    ```yaml
    AIRFLOW__CORE__PARALLELISM=2  # Máx 2 jobs concurrentes
    SPARK_DRIVER_MEMORY=2g
    SPARK_EXECUTOR_MEMORY=4g
    # Total: ~12GB usados, dejando 4GB libres para el SO
    ```

2.  **Particionamiento Mensual (`y/m`):**
    - **Antes**: Partición diaria (`y/m/d`) → Miles de archivos pequeños → Overhead masivo en GCS.
    - **Ahora**: Partición mensual → Archivos se acumulan en carpetas mensuales → ~30x más rápido.

3.  **Nombres Nativos de Spark:**
    - **Antes**: Renombrado manual (`part-0000.parquet`) → Copy+Delete en GCS (lento + race conditions).
    - **Ahora**: Nombres hash (`part-abc123.snappy.parquet`) → Escritura directa sin renombrado.

**Resultado:**
- ✅ **Estabilidad**: 99.9% (sin errores OOM).
- ✅ **Velocidad**: `silver_donaciones` bajó de 30 min a 3 min (~90% reducción).
- ✅ **Escalabilidad**: Arquitectura preparada para 10x volumen de datos.

### 📊 Guía de Tuning (Según Volumen de Datos)

| Volumen de Datos | Parallelism | Executor Memory | RAM VM | Tiempo Pipeline |
|------------------|-------------|-----------------|--------|-----------------|
| ~30k registros (actual) | 2 | 4g | 16GB | ~10-15 min |
| ~300k (10x) | 3 | 3g | 16GB | ~7-10 min |
| ~3M (100x) | 4 | 4g | 32GB | ~10-15 min |

**⚠️ Nota**: Siempre dejar 4GB+ libres para el Sistema Operativo.

---

## 🚀 Despliegue y Operaciones

### Entorno Cloud (Producción)
-   **Infraestructura:** GCP Compute Engine (Ubuntu 22.04, 4 vCPU, 16GB RAM).
-   **Seguridad:** Workload Identity (Sin llaves JSON almacenadas).
-   **Red:** Túnel SSH para acceso a UI de Airflow (Sin IP pública expuesta en puerto 8080).

### Cómo Ejecutar (VM)
1.  **Acceder a Airflow:**
    ```bash
    # Ejecutar en tu máquina local para crear el túnel
    gcloud compute ssh airflow-server-prod --zone=us-central1-a -- -L 8080:localhost:8080
    ```
    Ir a: `http://localhost:8080`

2.  **Desplegar Actualizaciones:**
    ```bash
    cd ~/data-plataform-fsp-spark-airflow
    git pull
    docker compose -f docker-compose.prod.yaml restart
    ```

3.  **Ejecutar Pipeline:**
    -   Activar (`Trigger`) el DAG `spdp_data_platform_main` en la UI de Airflow.
    -   Los logs se envían a Slack (`#data-alerts`) y a la UI de Airflow.

---

## 🛠️ Estructura del Proyecto

```bash
├── config/              # Configs específicas por entorno (Cloud vs Local)
├── dags/                # Definiciones de DAGs de Airflow
├── jobs/                # Jobs de PySpark
│   ├── silver/          # Lógica de Limpieza
│   ├── gold/            # Lógica de Agregación
│   └── utils/           # Helpers compartidos (GCS, Spark Session, Alertas)
├── scripts/             # Scripts de Python
│   ├── extract_from_supabase.py # Motor de Extracción
│   └── run_pipeline.sh          # Helper para ejecución manual
└── docker-compose.prod.yaml # Orquestación en Producción
```

---

## 📊 Métricas Clave
-   **Datos Históricos:** ~30,000 Registros Totales procesados en < 35 segundos (Extracción).
-   **Latencia del Pipeline:** ~10-15 minutos End-to-End (Optimizado desde 30+ min).
-   **Confiabilidad:** Reintentos automáticos, Dead Letter Queue (Cuarentena) para mala data, Alertas en Slack.

---

*Mantenedores: Vladislav Marinovich*
