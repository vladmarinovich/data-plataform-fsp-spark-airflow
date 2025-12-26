# 🏗️ Arquitectura del Proyecto

## Visión General

Este proyecto implementa un **Data Pipeline Cloud-Agnostic** basado en:

- **Apache Spark**: Procesamiento distribuido de datos
- **Apache Airflow**: Orquestación y scheduling
- **Medallion Architecture**: Bronze → Silver → Gold
- **Python**: Lenguaje principal para ETL

---

## Arquitectura de Capas

### 1. Bronze Layer (Raw Data)
**Propósito**: Almacenar datos crudos sin transformar

- **Formato**: Parquet (compresión Snappy)
- **Fuente**: APIs, Bases de datos, Archivos
- **Características**:
  - Datos tal cual vienen de la fuente
  - Inmutables (append-only)
  - Particionados por fecha de ingesta

**Ejemplo**:
```
data/raw/
  └── donaciones_mock.parquet
```

### 2. Silver Layer (Cleaned Data)
**Propósito**: Datos limpios y estandarizados

- **Formato**: Parquet particionado
- **Transformaciones**:
  - Validación de esquema
  - Conversión de tipos
  - Manejo de nulos
  - Deduplicación
  - Columnas derivadas (año, mes, día)

**Ejemplo**:
```
data/processed/silver/donaciones/
  ├── anio=2023/
  │   ├── mes=01/
  │   └── mes=02/
  └── anio=2024/
      └── mes=01/
```

### 3. Gold Layer (Business Aggregations)
**Propósito**: Métricas de negocio listas para consumo

- **Formato**: Parquet o Delta Lake
- **Transformaciones**:
  - Agregaciones por dimensiones
  - Cálculos de KPIs
  - Joins con dimensiones
  - Optimizado para queries analíticos

**Ejemplo**:
```
data/output/gold/
  └── donaciones_monthly/
      └── part-00000.parquet
```

---

## Flujo de Datos

```
┌─────────────────┐
│   Data Sources  │
│  (Supabase/API) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Extract Script │  ← Python + Pandas
│   (Python)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Parquet (Raw)
│   (Raw Data)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PySpark Job    │  ← Transformaciones
│  (Silver ETL)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Parquet Particionado
│ (Cleaned Data)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PySpark Job    │  ← Agregaciones
│   (Gold ETL)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  ← Métricas de Negocio
│ (Aggregations)  │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│  BI Tools /     │
│  Analytics      │
└─────────────────┘
```

---

## Componentes Principales

### 1. Config (`config/`)
Centraliza configuración por entorno:
- Paths de datos
- Credenciales (via .env)
- Constantes de negocio
- Configuración de Spark

### 2. Jobs (`jobs/`)
Scripts PySpark para transformaciones:
- `transform_donations.py`: Pipeline principal
- `utils/spark_session.py`: Factory de SparkSession
- `utils/data_quality.py`: Validaciones

### 3. DAGs (`dags/`)
Definiciones de Airflow para orquestación:
- Scheduling diario/horario
- Dependencias entre jobs
- Manejo de errores y retries

### 4. Scripts (`scripts/`)
Utilidades de desarrollo:
- `setup.sh`: Inicialización del proyecto
- `generate_mock_data.py`: Datos de prueba

---

## Patrones de Diseño

### 1. Idempotencia
Todos los jobs pueden ejecutarse múltiples veces sin efectos secundarios:
- Uso de `mode("overwrite")` con particiones
- Watermarks para procesamiento incremental
- Deduplicación basada en claves primarias

### 2. Particionamiento
Optimización de queries mediante particiones:
```python
df.write.partitionBy("anio", "mes").parquet(path)
```

### 3. Schema Evolution
Manejo de cambios en esquemas:
- Validación de columnas requeridas
- Columnas opcionales con defaults
- Versionado de esquemas

### 4. Data Quality
Validaciones en múltiples capas:
- Bronze: Validación básica de formato
- Silver: Validación de negocio
- Gold: Validación de métricas

---

## Configuración de Spark

### Local Development
```python
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```

### Production (Cloud)
```python
spark = SparkSession.builder \
    .master("yarn") \  # o "k8s://..." para Kubernetes
    .config("spark.executor.instances", "10") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

---

## Estrategias de Deployment

### 1. Local (Desarrollo)
- Spark standalone mode
- Datos en filesystem local
- Airflow en modo standalone

### 2. AWS
- EMR para Spark jobs
- S3 para data lake
- MWAA para Airflow
- Glue Catalog para metastore

### 3. GCP
- Dataproc para Spark jobs
- GCS para data lake
- Cloud Composer para Airflow
- BigQuery como warehouse

### 4. Azure
- HDInsight para Spark jobs
- Azure Blob Storage para data lake
- Azure Data Factory para orquestación
- Synapse Analytics como warehouse

---

## Monitoreo y Observabilidad

### Métricas Clave
- Tiempo de ejecución por job
- Cantidad de registros procesados
- Errores y retries
- Uso de recursos (CPU, memoria)

### Logging
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Procesando 1000 registros...")
```

### Alertas
- Jobs fallidos
- Latencia excesiva
- Calidad de datos degradada

---

## Mejores Prácticas

### 1. Código
- ✅ Usar type hints en Python
- ✅ Documentar con docstrings
- ✅ Tests unitarios para transformaciones
- ✅ Linting con Black y Flake8

### 2. Datos
- ✅ Particionamiento por fecha
- ✅ Compresión (Snappy para Parquet)
- ✅ Evitar small files problem
- ✅ Usar columnar formats (Parquet, ORC)

### 3. Performance
- ✅ Broadcast joins para tablas pequeñas
- ✅ Repartitioning antes de writes
- ✅ Caching de DataFrames reutilizados
- ✅ Adaptive Query Execution (AQE)

---

## Referencias

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
