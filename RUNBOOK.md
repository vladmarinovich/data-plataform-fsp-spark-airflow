# 🚒 SPDP Runbook de Operaciones

Este documento detalla los procedimientos operativos estándar y resolución de problemas para la **Salvando Patitas Data Platform**.

---

## 🚨 Procedimientos de Emergencia

### 1. El Pipeline Falló y no se recupera
Si Airflow reintentó 3 veces y sigue fallando:

1.  **Conectarse a la VM:**
    ```bash
    gcloud compute ssh airflow-server-prod --zone=us-central1-a -- -L 8080:localhost:8080
    ```
2.  **Revisar Logs de Docker:**
    ```bash
    cd ~/data-plataform-fsp-spark-airflow
    docker compose -f docker-compose.prod.yaml logs -f --tail=100 scheduler
    ```
3.  **Reiniciar Servicios (Soft Reset):**
    ```bash
    docker compose -f docker-compose.prod.yaml restart
    ```
4.  **Reinicio Total (Hard Reset - Si nada funciona):**
    ```bash
    docker compose -f docker-compose.prod.yaml down
    docker compose -f docker-compose.prod.yaml up -d
    ```

---

## 🛠️ Troubleshooting (Solución de Problemas)

### Error: `java.lang.OutOfMemoryError (OOM)`
*   **Síntoma:** El job de Spark muere súbitamente o el contenedor se reinicia.
*   **Causa:** La VM de 16GB se quedó sin RAM porque corrieron más de 2 jobs pesados a la vez.
*   **Solución:**
    1.  Verificar que `AIRFLOW__CORE__PARALLELISM` esté en `2` en `.env`.
    2.  Reducir `SPARK_EXECUTOR_MEMORY` a `3g` temporalmente.

### Error: `GCS 403 Forbidden`
*   **Síntoma:** "Access Denied" al intentar escribir en el bucket.
*   **Causa:** La cuenta de servicio de la VM perdió permisos o el token expiró.
*   **Solución:**
    1.  Verificar permisos de la VM: `gcloud compute instances describe airflow-server-prod`
    2.  Asegurar que el scope `storage-rw` esté activo.

### Error: `Parquet Column Type Mismatch`
*   **Síntoma:** Spark falla leyendo Bronze.
*   **Causa:** Alguien cambió un tipo de dato en Supabase (ej: `int` a `float`) y rompió el esquema histórico.
*   **Solución:**
    1.  Borrar la partición del día problemático en GCS Bronze.
    2.  Correr `extract_from_supabase.py` manualmente para ese día con `schema_casting` forzado.

---

## 📅 Mantimiento Rutinario

*   **Limpieza de Logs:** Airflow genera muchos logs. Ejecutar cada mes:
    ```bash
    docker exec -it airflow-webserver airflow db clean --days 30
    ```
*   **Actualización de Código:**
    ```bash
    git pull
    # Si cambiaron DAGs, Airflow los detecta solo.
    # Si cambiaron Jobs de Spark o Dockerfile, reiniciar contenedores.
    docker compose -f docker-compose.prod.yaml build
    docker compose -f docker-compose.prod.yaml up -d
    ```
