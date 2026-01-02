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

## ☁️ Cloud Deployment Day (2026-01-02) - Historias de Guerra

### 6. "Arquitectura Equivocada" (Apple Silicon vs Cloud AMD64)
**Síntoma:** Al levantar Docker en la VM (Ubuntu/AMD64), el error era inmediato: `exec /usr/bin/dumb-init: exec format error`.
**Diagnóstico:** La imagen Docker se construyó localmente en un Mac M1/M2/M3 (ARM64). Los procesadores son incompatibles con la VM de Google.
**Solución:**
- Implementar pipeline CI/CD con **GitHub Actions** (`.github/workflows/deploy.yml`).
- Configurar el workflow para construir explícitamente para `linux/amd64`.
- Esto automatiza el despliegue y asegura compatibilidad total.

### 7. Permisos de GCS sin JSON (The "No-Key" Challenge)
**Síntoma:** `403 Forbidden` al intentar escribir en el Bucket desde Spark, a pesar de que la VM tenía scope `cloud-platform`.
**Bloqueo:** No se podían crear llaves JSON por política de organización (`iam.disableServiceAccountKeyCreation`).
**Solución Correcta (Workload Identity):**
- En lugar de luchar contra la política o "hackear" llaves inseguras.
- Se otorgó el rol `roles/storage.admin` directamente a la **Cuenta de Servicio de la VM** (`...compute@developer.gserviceaccount.com`).
- Spark detecta automáticamente estas credenciales del entorno (Metadata Server) sin configuración extra.

### 8. Airflow Webserver "Crashloop" (Permisos de Disco)
**Síntoma:** `Permission denied: /opt/airflow/logs` o `airflow.cfg`. El contenedor se reiniciaba infinitamente.
**Causa:** Al montar el volumen `.` (directorio actual de la VM) en Docker, el usuario interno del contenedor (`airflow`, uid=1000) no tenía permiso para escribir en la carpeta propiedad del usuario `vladislavmarinovich`.
**Solución:**
- **Resetear Dueño:** `sudo chown -R 1000:0 .` (Darle propiedad al usuario 1000).
- **Permisos Totales:** `sudo chmod -R 777 .` (Solución rápida y efectiva en este contexto).
- **Limpieza:** `rm -f *.pid *.err` para eliminar bloqueos viejos.

### 9. Acceso Seguro vía Túnel SSH
**Síntoma:** La IP pública no respondía (y es inseguro abrir el puerto 8080 al mundo entero).
**Solución:** Port Forwarding vía SSH.
- Comando: `gcloud compute ssh airflow-server-prod ... -- -L 8080:localhost:8080`
- Permite gestionar Airflow desde `http://localhost:8080` de forma segura y encriptada.

---
*Última actualización: 02 Enero 2026*

### 10. Performance Bottleneck: Renombrado de Archivos en GCS
**Hallazgo (02-Ene-2026):**
El proceso `silver_donantes` tardó desproporcionadamente más que el resto.
**Causa:**
Al final de cada Job de Spark, ejecutamos `rename_spark_output` para tener un archivo limpio (ej. `donantes.parquet`).
En un sistema local (disco SSD), mover un archivo es instantáneo (cambio de puntero).
En **Google Cloud Storage (Object Store)**, "renombrar" NO existe. La API hace:
1. **Copiar** el objeto (Download/Upload interno).
2. **Borrar** el original.
Esto añade una sobrecarga de I/O brutal e innecesaria para tablas grandes.

**Acción Correctiva (Roadmap):**
- **Eliminar `rename_spark_output` en Prod:** Dejar que Spark escriba sus nativos `part-00000-...`. Son perfectamente válidos y ahorran minutos de ejecución.
- **Optimización de DAG:** Revisar dependencias para que las tareas pesadas ("Heavy Hitters") como Donantes no bloqueen el flujo crítico.


### 11.  en 
**Síntoma:** El Job de Spark corre todo bien, escribe los datos, pero se queda pegado o falla al final ("Renombrando archivos...").
**Causa:**
El script `file_utils.py` usa `google.cloud.storage.Client()` para renombrar archivos.
Esta librería de Python **requiere credenciales explícitas** (ADC) y no siempre hereda automáticamente el contexto de Hadoop/Spark.
Si no encuentra credenciales, puede quedarse buscando o fallar tras un timeout largo.

**Solución:**
Asegurarse que la librería `google-cloud-storage` tenga acceso a las credenciales de la VM.
Como ya dimos permisos IAM a la Service Account de la VM, esto *debería* funcionar si la librería detecta el Metadata Server.
Si falla, es mejor **deshabilitar el renombrado** en Cloud, ya que es una operación costosa (Copy+Delete).

