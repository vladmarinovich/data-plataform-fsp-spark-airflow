
# 📂 Índice de Archivos Clave

Estructura del proyecto y descripción de los componentes más importantes.

---

## 🛠️ Scripts Operativos (Capa Raw)

| Archivo | Descripción |
| :--- | :--- |
| **`scripts/extract_from_supabase.py`** | **Extractor ETL**. Conecta a Supabase, descarga datos incrementalmente usando `watermarks.json` y los guarda en parquet local. |
| **`scripts/upload_to_gcs.py`** | **Cargador Cloud**. Lee parquets locales, limpia tipos de datos (Int64, Timestamp Micros) y sube a GCS con particionamiento Hive. |
| **`scripts/create_external_tables.py`** | **Definidor DDL**. Crea/Actualiza tablas externas en BigQuery apuntando a GCS. |

## ⚙️ Configuración

| Archivo | Descripción |
| :--- | :--- |
| **`config/__init__.py`** | **Configuración Central**. Define rutas, nombres de buckets, schemas esperados y reglas de particionado. |
| **`.env`** | **Secretos**. Variables de entorno (no commitear). |
| **`watermarks.json`** | **Estado ETL**. Mantiene la fecha de última sincronización (`last_modified_at`) de cada tabla. |

## 🏗️ Spark Jobs (Transformación)

| Archivo | Descripción |
| :--- | :--- |
| `jobs/common.py` | Utilidades compartidas para Spark (Session, Logging). |
| `jobs/silver/*.py` | *(En desarrollo)* Scripts para transformar Raw -> Silver. |

## 📚 Documentación

| Archivo | Descripción |
| :--- | :--- |
| `README.md` | Documentación general de arquitectura y proyecto. |
| `INICIO_RAPIDO.md` | Guía "Cheat Sheet" para operar la carga diaria. |
| `RESUMEN_EJECUTIVO.md` | Estado del arte y logros del proyecto. |
