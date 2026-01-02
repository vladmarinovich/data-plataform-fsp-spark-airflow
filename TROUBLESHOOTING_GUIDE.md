# 🛠️ SPDP Data Platform - Troubleshooting Guide & Runbook

Este documento recopila los errores más comunes encontrados durante el desarrollo y operación del Data Pipeline (Airflow + Apache Spark + Supabase + GCS) y sus soluciones probadas.

---

## 🚨 Errores Críticos de Spark

### 1. `SparkException: [CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE]`
**Síntoma:** El Job falla al leer la capa `Raw` o `Silver` indicando conflicto entre tipos (ej. `BigInt` y `Double`).
**Causa:**
- Cambio en el esquema de la fuente (Supabase).
- Mezcla de archivos Parquet antiguos (con esquema viejo) y nuevos en la misma carpeta del Data Lake.
- Inferencia de tipos de Spark fallando al leer múltiples particiones.

**Solución (Runbook):**
1.  **Identificar la tabla afectada** (ej. `casos`, `donaciones`).
2.  **Limpiar la carpeta en GCS:**
    ```bash
    gsutil rm -r gs://salvando-patitas-spark/lake/raw/<tabla>/*
    ```
3.  **Resetear Watermark (Opcional):** Si es necesario re-procesar todo el histórico.
4.  **Ejecutar Airflow:** Clear Task `extract_from_supabase` para iniciar una carga limpia.

---

### 2. `Task received SIGTERM signal` / `TimeoutException`
**Síntoma:** La tarea en Airflow se interrumpe abruptamente o muestra un error de timeout ("Cannot receive any reply...").
**Causa Root:**
- **Recursos Insuficientes:** El contenedor de Docker se quedó sin RAM/CPU (común en desarrollo local con otras apps abiertas).
- **Latencia de Red:** Spark tardó demasiado listando archivos en GCS debido a una conexión lenta.
- **Zombie Process:** Un proceso anterior de Spark no murió bien y bloqueó recursos.

**Solución:**
1.  **Cerrar aplicaciones pesadas** en el Host (Juegos, IDEs pesados, Chrome tabs).
2.  **Reiniciar Docker:**
    ```bash
    docker-compose down && docker-compose up -d
    ```
3.  **Aumentar recursos:** (Si persiste) Aumentar RAM asignada a Docker Desktop (mínimo 6GB para Spark).
4.  **Migrar a Cloud:** Usar una VM dedicada (e2-standard-4) soluciona esto definitivamente.

---

## 🔐 Errores de Autenticación & Cloud

### 3. `InvalidSignatureError` / `401 Unauthorized` / `Refresh Token`
**Síntoma:** Fallos al acceder a GCS o BigQuery desde dentro del contenedor.
**Causa:**
- Las credenciales ADC (`application_default_credentials.json`) expiraron o rotaron.
- El reloj del sistema (Docker vs Host) está desincronizado.

**Solución:**
1.  **Refrescar Credenciales en el Host:**
    ```bash
    gcloud auth application-default login
    ```
2.  **Reiniciar contenedores** (para que monten el nuevo archivo JSON).
    ```bash
    docker-compose restart
    ```

---

## 🌪️ Errores de Airflow

### 4. `NameError: name 'XYZ' is not defined`
**Síntoma:** El DAG falla inmediatamente al iniciar una tarea Python.
**Causa:**
- Error en el código Python (falta un `import`).
- Airflow Scheduler no ha refrescado el código (tiene un delay por defecto).

**Solución:**
1.  **Revisar Imports:** Asegurar que todas las variables/módulos se importan explícitamente.
2.  **Esperar:** Darle 30-60s al Scheduler para detectar cambios.
3.  **Reiniciar Scheduler:** Si se pone terco.

### 5. `DAG scheduling skipped, record locked`
**Síntoma:** El DAG no avanza aunque no haya errores visibles.
**Causa:**
- La base de datos de Airflow (Postgres) se sobrecargó o reinició, dejando un "lock" en la fila del DAG Run.

**Solución:**
- Generalmente **se arregla solo** tras unos minutos.
- Si no, reiniciar el Scheduler (`docker-compose restart airflow-scheduler`).

---

## 🏗️ Decisiones de Arquitectura (Contexto)

### Estrategia de Carga Híbrida
- **Tablas Fact (Donaciones, Gastos):** Incremental (Watermark) + Append.
- **Tablas Dim (Donantes, Casos):** Incremental Extracción (Watermark) + **Full Overwrite** Silver (Deduplicación).
  * *Por qué:* Garantiza la "versión única de la verdad" y maneja cambios en dimensiones (SCD Type 1) sin complejidad extra.

---
*Última actualización: Enero 2026*
