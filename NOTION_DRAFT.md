# 🏗️ PySpark High-Performance Pipeline
*(Architecture & Documentation)*

> **Status:** 🟢 Production Ready
> **Stack:** Python 3.11 | Apache Spark 3.5 | Airflow 2.10 | Google Cloud Platform

---

## 1. 🔭 Arquitectura (Lakehouse)

Este pipeline implementa una arquitectura **Medallion (Bronze/Silver/Gold)** desplegada en **Google Compute Engine** usando Contenedores Docker.

### 🧩 Diagrama de Flujo
```mermaid
graph LR
    SRC[Supabase DB] -->|Extract| BRONZE[Lake: Raw (Parquet)]
    BRONZE -->|Spark Clean| SILVER[Lake: Silver (Delta/Parquet)]
    SILVER -->|Spark Agg| GOLD[Lake: Gold (Star Schema)]
    GOLD -->|Load| BQ[BigQuery Warehouse]
    BQ -->|Connect| VIZ[Looker Studio]
```

### 💧 Capas de Datos
-   **Bronze (Raw):** Datos crudos extraídos incrementalmente. Particionamiento por fecha de negocio (`last_modified_at`).
-   **Silver (Refined):** Datos limpios, deduplicados y tipados fuertemente. Calidad de datos aplicada (Reglas de negocio).
-   **Gold (Curated):** Modelos dimensionales (Hechos y Dimensiones) listos para BI.

---

## 2. ⚡ Performance Wins ("Historias de Guerra")

> 💡 **Logro Principal:** Reducción del tiempo de ejecución End-to-End de **32 minutos a < 5 minutos** (700% Boost).

### 🚀 Optimización GCS (Object Storage)
-   **El Problema:** Apache Spark trata a GCS como un sistema de archivos tradicional (POSIX). El mecanismo de "commit" de parquets implica escribir a un temporal y luego "Renombrar". En la nube, **Renombrar = Copiar + Borrar**, lo cual es extremadamente lento para miles de archivos pequeños.
-   **La Solución:** Implementamos un protocolo de escritura personalizado (`file_utils.py`) que deshabilita el rernombrado costoso cuando detecta el entorno `ENV=cloud`, escribiendo directamente al destino final.

### 🔄 Estrategia de Carga Híbrida
-   **El Reto:** Los datos de dimensiones (Donantes, Casos) cambian frecuentemente y necesitamos historia completa.
-   **La Solución:**
    -   **Tablas Pequeñas/Medianas (Dimensiones):** Full Load Snapshot en cada ejecución (Garantiza consistencia 100%).
    -   **Tablas Grandes (Hechos):** Carga Incremental basada en High-Watermark (Eficiencia).

---

## 3. 🛠️ Stack Tecnológico

| Componente | Tecnología | Rol |
| :--- | :--- | :--- |
| **Orquestador** | Apache Airflow 2.10 | Gestión de dependencias, reintentos y alertas (Slack). |
| **Procesamiento** | PySpark 3.5 | Motor de procesamiento en memoria distribuida. |
| **Storage** | Google Cloud Storage | Data Lake escalable y barato. |
| **Warehouse** | BigQuery | Capa de servicio para consultas SQL rápidas. |
| **Infraestructura** | GCE VM (Ubuntu) | Servidor `e2-standard-4` (4 vCPU, 16GB RAM). |
| **Seguridad** | Workload Identity | Autenticación sin llaves JSON (Service Account Attach). |

---

## 4. 📊 Métricas Clave

| Métrica | Valor | Notas |
| :--- | :---: | :--- |
| **Registros Históricos** | **30,000+** | Donantes, Donaciones, Gastos, Casos. |
| **Latencia Pipeline** | **~4 min** | Desde extracción hasta BigQuery. |
| **Volumen Diario** | ~100MB | Escalable a TBs sin cambios de código. |
| **Costo Operativo** | **Bajo** | Uso de VM volátil + Storage Coldline. |

---

## 5. 📘 Runbook (Operaciones)

### 🚨 Alertas (Slack)
El pipeline notifica al canal `#data-alerts`:
-   ✅ **Success:** Resumen de tiempos y registros.
-   ❌ **Failure:** Link directo a los logs de Airflow y Tag al equipo.

### 🔄 Disaster Recovery
Si el Lake se corrompe o se necesita reprocesar todo:
1.  Borrar carpeta `checkpoints/watermarks` en GCS.
2.  Trigger DAG manual.
3.  El sistema detecta automáticamente "Clean Slate" y reimporta toda la historia.

```bash
# Comando de Pánico (Reset Total)
gsutil -m rm -r gs://salvando-patitas-spark/checkpoints/*
```
