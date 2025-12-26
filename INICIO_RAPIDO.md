
# ⚡ INICIO RÁPIDO - Operación Diaria (Raw Layer)

Guía para ejecutar la carga de datos diaria (Capa Raw) de Salvando Patitas.

---

## 📋 Pre-requisitos

1.  Estar en directorio del proyecto:
    ```bash
    cd pyspark-airflow-data-platform
    ```
2.  Activar entorno virtual:
    ```bash
    source venv/bin/activate
    ```
3.  Tener credenciales configuradas en `.env` (Supabase + GCP).

---

## 🚀 Ejecución de Carga Diaria (RAW)

Para sincronizar Supabase con BigQuery (Data Lake):

### 1️⃣ Extraer Datos Nuevos (Incremental)
Descarga solo lo modificado desde la última ejecución (guarda estado en `watermarks.json`).

```bash
python scripts/extract_from_supabase.py
```

### 2️⃣ Subir a GCS y Particionar
Sube parquets a `gs://salvando-patitas-spark-raw/` organizados por `anio/mes/dia`. Corrige tipos de dato (IDs, Fechas).

```bash
python scripts/upload_to_gcs.py
```

### 3️⃣ Actualizar BigQuery External Tables
Asegura que BigQuery reconozca nuevas particiones y cambios de esquema.

```bash
python scripts/create_external_tables.py
```

---

## 🔍 Verificación

Para confirmar que todo salió bien, ejecuta este query en BigQuery:

```sql
-- Verificar carga de hoy
SELECT table_name, count(*) as total_rows 
FROM `salvando-patitas-de-spark.raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'raw_%'
GROUP BY table_name;
```

---

## 🛠️ Solución de Problemas Frecuentes

*   **Error por falta de credenciales**: Revisa `export GOOGLE_APPLICATION_CREDENTIALS="..."`.
*   **Error de tipos en BigQuery**: Asegúrate de haber ejecutado el paso 3 (`create_external_tables.py`) que recrea las definiciones.
*   **Quiero recargar todo desde cero**: Borra `watermarks.json` y ejecuta los pasos 1, 2 y 3.

