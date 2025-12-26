
# 🚀 PySpark + GCP Data Platform (Salvando Patitas)

> **Modern Data Platform** diseñada para la ingesta, transformación y análisis de datos de Salvando Patitas, utilizando **Supabase**, **Google Cloud Storage (GCS)**, **BigQuery** y **Apache Spark**.

---

## 📋 Tabla de Contenidos

- [Arquitectura](#-arquitectura)
- [Workflow de Datos](#-workflow-de-datos)
- [Requisitos Previos](#-requisitos-previos)
- [Instalación](#-instalación)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Ejecución Capa RAW](#-ejecución-capa-raw)
- [Variables de Entorno](#-variables-de-entorno)

---

## 🏗️ Arquitectura

Este proyecto implementa una arquitectura **Medallion (Bronze/Silver/Gold)** sobre Google Cloud Platform:

1.  **Fuente**: Supabase (PostgreSQL).
2.  **Raw (Bronze)**: Datos crudos en formato **Parquet** almacenados en **GCS**, particionados por `anio/mes/dia`. Expuestos como *External Tables* en **BigQuery**.
3.  **Silver (En proceso)**: Datos limpios, deduplicados y tipados (Spark Jobs).
4.  **Gold (En proceso)**: Agregaciones de negocio para dashboards.

---

## 🔄 Workflow de Datos

El pipeline actual cubre la capa RAW completa:

1.  **Extracción**: `scripts/extract_from_supabase.py`
    *   Descarga incremental (usando `watermarks.json`) o Full Load.
    *   Guarda localmente en `data/raw/*.parquet`.
2.  **Carga a GCS**: `scripts/upload_to_gcs.py`
    *   Sube archivos a `gs://salvando-patitas-spark-raw/`.
    *   Aplica **Particionamiento Hive** (`anio=YYYY/mes=MM/dia=DD`) para tablas transaccionales.
    *   Convierte tipos críticos (IDs a INT64, Fechas a UTC Microseconds).
3.  **Definición**: `scripts/create_external_tables.py`
    *   Crea o actualiza tablas externas en BigQuery (`raw_donaciones`, `raw_casos`, etc.).
    *   Configura detección de particiones automática.

---

## ✅ Requisitos Previos

*   **Python 3.9+**
*   **Google Cloud SDK** (gcloud) autenticado.
*   **Cuenta de Servicio GCP** (o credenciales de usuario con permisos de Storage Admin y BigQuery Admin).

---

## 🔧 Instalación

### 1. Clonar y Configurar entorno

```bash
git clone <repo-url>
cd pyspark-airflow-data-platform
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno

Copiar `.env.example` a `.env` y configurar:

```ini
# GCP
GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json" # Opcional si usas gcloud auth application-default

# Supabase
SUPABASE_URL="https://tu-proyecto.supabase.co"
SUPABASE_KEY="tu-service-role-key"

# Spark
SPARK_MASTER="local[*]"
```

---

## 📁 Estructura del Proyecto

```
pyspark-airflow-data-platform/
│
├── config/                  # Configuración central (paths, schemas)
├── jobs/                    # Spark Jobs (Silver/Gold)
├── scripts/
│   ├── extract_from_supabase.py   # ETL Supabase -> Local
│   ├── upload_to_gcs.py           # ETL Local -> GCS (Partitioned)
│   └── create_external_tables.py  # DDL BigQuery External
├── data/                    # Almacenamiento temporal (gitignored)
├── logs/                    # Logs de ejecución
├── requirements.txt         # Dependencias
└── watermarks.json          # Estado de extracción incremental
```

---

## 🚀 Ejecución Capa RAW

Para actualizar la capa Raw desde cero o incrementalmente:

```bash
# 1. Extraer datos nuevos de Supabase
python scripts/extract_from_supabase.py

# 2. Subir y particionar en GCS
python scripts/upload_to_gcs.py

# 3. Actualizar definiciones en BigQuery (si cambió el schema)
python scripts/create_external_tables.py
```

---

## 🛡️ Seguridad

*   **NUNCA** subir credenciales al repositorio.
*   Usar `.env` para secretos locales.
*   El archivo `watermarks.json` mantiene el estado de la extracción, no borrar a menos que se desee un Full Load.

---

**Hecho con 💜 para Salvando Patitas**
