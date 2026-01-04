# 📋 Estado de Sesión - Handover

**Fecha:** 2026-01-03
**Objetivo:** Refactor Pipeline Incremental + Nuclear Test
**Estado:** 🛑 PAUSADO (VM Apagada)

## ✅ Logros Hoy
1. **Infraestructura Reparada:**
   - Se aumentó memoria de Docker (Scheduler 10GB, Spark 4GB) para evitar `OOM Killed`.
   - Se corrigieron permisos en `data/` y `logs/`.
2. **Código Listo:**
   - Refactor `silver_*.py` y `gold_*.py` a incremental terminados.
   - Watermark global implementado.
3. **Datos:**
   - "Opción Nuclear" ejecutada (GCS limpio).
   - Tareas `repartition_raw` exitosas.

## ⏭️ Pasos para la Siguiente Sesión
La VM `airflow-server-prod` está **APAGADA**. Al iniciar:

1. **Encender VM:**
   ```bash
   gcloud compute instances start airflow-server-prod --zone=us-central1-a --project=salvando-patitas-de-spark
   ```

2. **Reanudar Pipeline:**
   Conectarse por SSH y limpiar la tarea fallida (que falló por memoria antes del fix) para que reintente con los 10GB nuevos.
   ```bash
   # Esperar a que Docker inicie (appx 1 min)
   docker exec data-plataform-fsp-spark-airflow_airflow-scheduler_1 airflow tasks clear spdp_data_platform_main -t silver_donantes -d -y
   ```

3. **Validación:**
   - Esperar que el DAG termine (debería ser rápido con la nueva RAM).
   - Verificar creación de `gs://salvando-patitas-spark/state/watermarks.json`.
   - Verificar datos en BigQuery.

4. **Apagar:**
   - El DAG tiene un task `stop_instance` al final, así que **debería apagarse sola** si todo sale bien. Si falla, apagar manual.
