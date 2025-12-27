# 🐾 Salvando Patitas - Data Platform (PySpark + Airflow)

Pipeline de datos cloud-agnostic usando Apache Spark y Apache Airflow para procesamiento ETL de datos de rescate animal.

## 🏗️ Arquitectura

```
Supabase (Transactional DB) 
    ↓
Extract (Pandas - rápido)
    ↓
Raw Layer (Parquet)
    ↓
Silver Layer (Spark - Data Quality + Transformaciones)
    ↓
Gold Layer (Spark - Dimensiones + Hechos + Features)
    ↓
BigQuery (Análisis/BI) [TODO]
```

## 📊 Estado Actual

### ✅ Funcionando
- **Extracción**: 6 tablas desde raw mock data (5 segundos)
- **Silver Layer**: 6 transformations con Data Quality Assertions
- **Gold Layer**: Dimensiones, Hechos, Features, Dashboards
- **Orquestación**: Airflow con DAG completo
- **Cuarentena**: Sistema de quarantine para datos inválidos

### ⚠️ Issues Conocidos

**Performance Local (Docker + Spark):**
- **Tiempo actual**: ~27 minutos end-to-end
- **Tiempo esperado en cloud**: 5-10 minutos
- **Problema**: Overhead de Spark para datasets pequeños en laptop

**Configuración Actual:**
- RAM: 8GB Docker
- Spark Driver: 3GB
- Spark Executor: 3GB
- Shuffle Partitions: 8
- Cores: local[2]

### 📝 Optimizaciones Aplicadas

```python
# jobs/utils/spark_session.py
.config("spark.sql.shuffle.partitions", "8")  # Default: 200
.config("spark.default.parallelism", "4")     # Reduce overhead
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
.config("spark.sql.adaptive.enabled", "true")
```

## 🚀 Quick Start

### Prerequisitos
```bash
docker
docker-compose
Python 3.11+
```

### Levantar Ambiente Local
```bash
# 1. Levantar Airflow + PostgreSQL
docker compose up -d

# 2. Acceder a Airflow UI
open http://localhost:8080
# Usuario: admin
# Password: admin

# 3. Trigger DAG manualmente
# Click en "spdp_data_platform_main" → Trigger
```

## 📁 Estructura del Proyecto

```
├── config/              # Configuración global (paths, Spark, etc)
├── dags/                # Airflow DAGs
│   └── spdp_main_pipeline.py
├── jobs/
│   ├── silver/         # Transformaciones Silver + DQ
│   ├── gold/           # Agregaciones Gold
│   └── utils/          # Spark session, helpers
├── scripts/
│   ├── quick_mock_data.py      # Generador de datos mock (Pandas)
│   └── inspect_quarantine.py   # Revisar datos rechazados
└── data/
    └── lake/
        ├── raw/        # Bronze layer (Parquet)
        ├── silver/     # Silver layer (Parquet)
        ├── gold/       # Gold layer (Parquet)
        └── quarantine/ # DQ rejected records
```

## 🛡️ Data Quality

Implementado en Silver Layer:

**Donaciones:**
- Email válido (contiene @)
- ID no nulo
- Fecha posterior a 2010

**Casos:**
- ID no nulo
- Fecha ingreso válida (2010-hoy)
- Nombre no default

**Hogares:**
- Cupo no negativo
- Tarifa no negativa
- Nombre obligatorio

Registros que fallan → `data/lake/quarantine/{table}/`

## 🎯 Próximos Pasos Sugeridos

### 1. **Performance** (CRÍTICO)
- [ ] Evaluar deployment a Google Cloud Dataproc
- [ ] Considerar dbt para transformaciones simples
- [ ] Profile Spark jobs para identificar bottlenecks

### 2. **BigQuery Integration**
- [ ] Agregar carga de Gold → BigQuery
- [ ] Configurar external tables en GCS
- [ ] Automatizar schema sync

### 3. **Monitoring**
- [ ] Agregar métricas de calidad de datos
- [ ] Dashboard de ejecución en Airflow
- [ ] Alertas de fallo

## 🤝 Para Reviewers

**Pregunta principal**: 
> ¿Cómo optimizar tiempos de ejecución para datasets < 1000 registros sin sacrificar la arquitectura Spark/Airflow?

**Contexto**:
- Objetivo: Pipeline production-ready para portafolio
- Datasets actuales: 50-200 registros por tabla
- Growth esperado: 50K+ registros
- Must-have: Spark (skill requerido para vacantes)

**Áreas de review**:
1. Configuración de Spark (¿sobrecarga innecesaria?)
2. Estrategia de particionamiento
3. Alternativas híbridas (Pandas en local, Spark en cloud)
4. Arquitectura de DAG (¿dependencias optimizadas?)

---

## 📚 Stack Tecnológico

- **Orquestación**: Apache Airflow 2.10
- **Procesamiento**: Apache Spark 3.5 (PySpark)
- **Storage**: Parquet (columnar)
- **BD Transaccional**: Supabase (PostgreSQL)
- **Metadata**: Airflow PostgreSQL
- **Cloud Target**: Google Cloud (Dataproc + Composer + BigQuery)

## 📄 Licencia

MIT
