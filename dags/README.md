# 🚀 DAG Principal - Salvando Patitas Data Platform

## 📊 Descripción

Pipeline de datos completo que extrae información desde Supabase (CRM), la procesa en capas (Silver/Gold) y la carga en BigQuery para análisis en Looker Studio.

## 🏗️ Arquitectura del Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 0: EXTRACCIÓN (Bronze/Raw)                            │
├─────────────────────────────────────────────────────────────┤
│ extract_from_supabase → repartition_raw                     │
│ • Extrae desde Supabase con watermark incremental          │
│ • Particiona por fechas de negocio (y/m o y/m/d)           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 1: LIMPIEZA (Silver)                                  │
├─────────────────────────────────────────────────────────────┤
│ • donantes, casos, donaciones, gastos, proveedores, hogar   │
│ • Normalización de estados                                  │
│ • Defaults (país='Colombia', canal='desconocido')           │
│ • Deduplicación                                             │
│ • Validación de calidad                                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 2: DIMENSIONES Y HECHOS (Gold Base)                   │
├─────────────────────────────────────────────────────────────┤
│ Dimensiones:                                                │
│ • dim_calendario (base temporal)                            │
│ • dim_donantes (con país y canal)                           │
│ • dim_casos, dim_proveedores, dim_hogar_de_paso            │
│                                                             │
│ Hechos:                                                     │
│ • fact_donaciones (solo estado='completada')                │
│ • fact_gastos (solo estado='pagado')                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 3: FEATURES (Gold Metrics)                            │
├─────────────────────────────────────────────────────────────┤
│ • feat_donantes (RFM, lifetime value)                       │
│ • feat_casos (métricas de rescate)                          │
│ • feat_proveedores (análisis de gastos)                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 4: DASHBOARDS (Gold Presentation)                     │
├─────────────────────────────────────────────────────────────┤
│ • dashboard_donaciones (con RFM y segmentación)             │
│ • dashboard_gastos (análisis de proveedores)                │
│ • dashboard_financiero (consolidado)                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ NIVEL 5: CARGA Y WATERMARK                                  │
├─────────────────────────────────────────────────────────────┤
│ load_to_bigquery → update_watermark                         │
│ • Carga dashboards y features a BigQuery                    │
│ • Actualiza watermark para próxima ejecución                │
└─────────────────────────────────────────────────────────────┘
```

## ⏰ Programación

- **Schedule**: Domingos a las 23:30 UTC
- **Catchup**: Deshabilitado
- **Max Active Runs**: 1 (evita ejecuciones concurrentes)
- **Max Active Tasks**: 4 (paralelismo controlado)

## 🔧 Configuración

### Variables de Entorno Requeridas

```bash
ENV=cloud
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-service-account.json
```

### Dependencias

- PySpark 3.5+
- Python 3.11
- Java 17 (para PySpark)
- Google Cloud SDK
- Supabase Python Client

## 📈 Métricas Clave

### Datos Procesados (Q1 2023)
- **Donaciones**: 851 registros
- **Monto Total**: $211,408,500.00
- **Donantes**: 10,003 registros históricos
- **Casos**: 262 registros

### Calidad de Datos
- **País NULL**: 0% (default a 'Colombia')
- **Canal Origen NULL**: 0% (default a 'desconocido')
- **Estados Normalizados**: 100%

## 🚨 Monitoreo

### Logs Importantes

```bash
# Ver logs del DAG
docker-compose logs -f airflow-scheduler

# Ver logs de una tarea específica
airflow tasks logs spdp_data_platform_main extract_from_supabase 2025-12-28
```

### Alertas

El DAG falla si:
- No se puede conectar a Supabase
- Hay errores en validación de calidad (Silver)
- Falla la carga a BigQuery
- No se puede actualizar el watermark

## 🔄 Ejecución Manual

```bash
# Trigger manual del DAG
airflow dags trigger spdp_data_platform_main

# Ejecutar solo una tarea
airflow tasks test spdp_data_platform_main extract_from_supabase 2025-12-28
```

## 📊 Salidas

### BigQuery Tables

- `gold.dashboard_donaciones` - Dashboard principal de donaciones
- `gold.dashboard_gastos` - Dashboard de gastos
- `gold.dashboard_financiero` - Consolidado financiero
- `gold.feat_donantes` - Features de donantes
- `gold.feat_casos` - Features de casos
- `gold.feat_proveedores` - Features de proveedores

### GCS Buckets

- `gs://salvando-patitas-spark/lake/raw/` - Datos crudos
- `gs://salvando-patitas-spark/lake/silver/` - Datos limpios
- `gs://salvando-patitas-spark/lake/gold/` - Datos analíticos
- `gs://salvando-patitas-spark/state/watermarks.json` - Estado del pipeline

## 🐛 Troubleshooting

### Error: "Watermark not found"
```bash
# Resetear watermark
gsutil cp watermarks_init.json gs://salvando-patitas-spark/state/watermarks.json
```

### Error: "Partition limit exceeded"
- Tablas maestras (donantes, casos) usan particionamiento y/m
- Tablas transaccionales (donaciones, gastos) usan y/m/d

### Error: "BigQuery load failed"
- Verificar credenciales de GCP
- Verificar que el dataset 'gold' existe
- Revisar permisos del service account

## 📚 Referencias

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
