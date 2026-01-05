# 📂 Índice del Proyecto SPDP

Este proyecto consta de **65 archivos** esenciales distribuidos así:

## 1. Orquestación (Airflow)
- dags/__init__.py
- dags/spdp_main_pipeline.py

## 2. Lógica de Negocio (Spark Jobs)
- jobs/transform_donations.py
- jobs/silver/hogar_de_paso.py
- jobs/silver/gastos.py
- jobs/silver/donantes.py
- jobs/silver/casos.py
- jobs/silver/donaciones.py
- jobs/silver/proveedores.py
- jobs/__init__.py
- jobs/utils/alerts.py
- jobs/utils/spark_session.py
- jobs/utils/watermark.py
- jobs/utils/__init__.py
- jobs/utils/file_utils.py
- jobs/utils/data_quality.py
- jobs/gold/dim_proveedores.py
- jobs/gold/dim_donantes.py
- jobs/gold/dashboard_financiero.py
- jobs/gold/upload_to_bucket.py
- jobs/gold/feat_proveedores.py
- jobs/gold/dim_calendario.py
- jobs/gold/feat_donantes.py
- jobs/gold/dim_casos.py
- jobs/gold/fact_donaciones.py
- jobs/gold/dim_hogar_de_paso.py
- jobs/gold/fact_gastos.py
- jobs/gold/feat_casos.py
- jobs/gold/dashboard_gastos.py
- jobs/gold/dashboard_donaciones.py
- jobs/common.py

## 3. Scripts de Ingesta
- scripts/debug_gcs_watermark.py
- scripts/verify_gold_dims.py
- scripts/verify_gcs_files.py
- scripts/clean_bucket.py
- scripts/debug_gastos_leak.py
- scripts/manage_watermark.py
- scripts/verify_gold_data.py
- scripts/force_extract_casos.py
- scripts/test_extraction.py
- scripts/debug_donantes_nulls.py
- scripts/test_gcs_spark.py
- scripts/debug_donantes_count.py
- scripts/quick_mock_data.py
- scripts/verify_cloud_data.py
- scripts/reextract_donantes.py
- scripts/load_to_bigquery.py
- scripts/debug_casos_missing.py
- scripts/upload_to_gcs.py
- scripts/extract_from_supabase.py
- scripts/generate_mock_data.py
- scripts/run_gold_layer.py
- scripts/clean_raw_dims.py
- scripts/repartition_raw.py
- scripts/analyze_donantes_leak.py
- scripts/setup_gcp.py
- scripts/upload_to_gcs_and_bq.py
- scripts/create_external_tables.py
- scripts/inspect_quarantine.py
- scripts/finalize_pipeline.py

## 4. Documentación & Portfolio
- docs/EXTRACCION_SUPABASE.md
- docs/ARCHITECTURE.md
- docs/QUICKSTART.md
- PORTFOLIO.md
- README.md

## 5. Configuración Infra
- ./docker-compose.yaml
- ./docker-compose.prod.yaml
