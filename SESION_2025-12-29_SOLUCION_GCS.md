# 🔧 Solución: Autenticación GCS con Python SDK

**Fecha**: 29-Dic-2025  
**Problema**: Error 401 al subir archivos a GCS desde Docker  
**Solución**: Migración de `gsutil` a `google-cloud-storage` Python SDK

---

## 🔍 DIAGNÓSTICO DEL PROBLEMA

### Problema Original
```
ServiceException: 401 Anonymous caller does not have storage.objects.create access
```

### Causa Raíz
1. **Credenciales ADC (Application Default Credentials)**:
   - Tipo: Credenciales de **usuario** (no service account)
   - Archivo: `~/.config/gcloud/application_default_credentials.json`
   - Creadas con: `gcloud auth application-default login`

2. **Limitación en Docker**:
   - `gsutil` requiere sesión activa de `gcloud auth`
   - ADC de usuario NO funciona automáticamente con `gsutil` en contenedores
   - El archivo JSON está montado pero NO activado

3. **Política Organizacional**:
   - La organización bloquea creación de service account keys
   - Error: "An Organization Policy that blocks service accounts key creation has been enforced"
   - No es posible crear service account tradicional

---

## ✅ SOLUCIÓN IMPLEMENTADA

### Migración a Python SDK

**Archivo modificado**: `scripts/extract_from_supabase.py` (líneas 268-291)

**Antes (gsutil)**:
```python
cmd = f"gsutil -m cp -r {table_path}/* {gcs_path}/"
result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
```

**Después (Python SDK)**:
```python
from google.cloud import storage

storage_client = storage.Client()  # Usa automáticamente ADC
bucket = storage_client.bucket("salvando-patitas-spark")

for root, dirs, files in os.walk(table_path):
    for file in files:
        local_file = os.path.join(root, file)
        rel_path = os.path.relpath(local_file, table_path)
        blob_path = f"lake/raw/{table_name}/{rel_path}"
        
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_file)
```

---

## 🎯 VENTAJAS DE LA SOLUCIÓN

1. ✅ **Funciona con ADC**: No requiere service account keys
2. ✅ **Cumple políticas**: Compatible con restricciones organizacionales
3. ✅ **Más robusto**: Mejor manejo de errores que subprocess
4. ✅ **Más rápido**: No hay overhead de spawning procesos
5. ✅ **Mejor logging**: Control granular de qué archivos se suben
6. ✅ **Pythonic**: Código más limpio y mantenible

---

## 📋 CONFIGURACIÓN ACTUAL

### Docker Compose
```yaml
volumes:
  - ~/.config/gcloud:/root/.config/gcloud:ro

environment:
  - GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json
  - ENV=cloud
```

### Requirements
```
google-cloud-storage>=2.13.0  # ✅ Ya incluido
```

---

## 🚀 PRÓXIMOS PASOS

### 1. Reconstruir Imagen Docker
```bash
cd "/Users/vladislavmarinovich/Library/CloudStorage/GoogleDrive-consultor@vladmarinovich.com/Shared drives/Vladislav/Salvando Patitas (SPDP) S-A/pyspark-airflow-data-platform"

docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### 2. Verificar Logs
```bash
# Ver logs del scheduler
docker-compose logs -f airflow-scheduler

# Ver estado de contenedores
docker-compose ps
```

### 3. Trigger DAG Manual
```bash
# Desde la UI: http://localhost:8080
# O desde CLI:
docker-compose exec -T airflow-scheduler airflow dags trigger spdp_data_platform_main
```

### 4. Validar Subida a GCS
```bash
# Verificar archivos en GCS
gsutil ls -r gs://salvando-patitas-spark/lake/raw/

# O desde Python
python3 scripts/verify_cloud_data.py
```

---

## 🔐 SEGURIDAD

### Credenciales ADC
- ✅ **Archivo protegido**: En `.gitignore`
- ✅ **Montado read-only**: `:ro` en docker-compose
- ✅ **Scope limitado**: Solo permisos de tu cuenta GCP

### Alternativa Futura (Si se levanta la política)
Si en el futuro se permite crear service account keys:
1. Crear service account con roles:
   - Storage Object Admin
   - BigQuery Data Editor
   - BigQuery Job User
2. Descargar key JSON
3. Actualizar `GOOGLE_APPLICATION_CREDENTIALS` en docker-compose
4. El código Python SDK funcionará igual (sin cambios)

---

## 📊 TESTING

### Test Local (Antes de Docker)
```bash
# Activar venv
source venv/bin/activate

# Test de autenticación
python3 -c "from google.cloud import storage; print(storage.Client().list_buckets())"

# Debe listar tus buckets sin error
```

### Test en Docker
```bash
# Entrar al contenedor
docker-compose exec airflow-scheduler bash

# Test de autenticación
python3 -c "from google.cloud import storage; client = storage.Client(); print('✅ Auth OK')"
```

---

## 💡 NOTAS IMPORTANTES

1. **ADC Expiration**: Las credenciales ADC pueden expirar. Si ves errores de auth:
   ```bash
   gcloud auth application-default login
   ```

2. **Permisos**: Asegúrate de tener los roles necesarios en GCP:
   - Storage Object Admin (en el bucket)
   - BigQuery Data Editor (en el dataset)

3. **Logs Detallados**: El nuevo código imprime cada archivo subido:
   ```
   ✅ Uploaded: lake/raw/donaciones/y=2023/m=01/file.parquet
   ```

---

## ✅ CHECKLIST DE VALIDACIÓN

Después de implementar:

- [ ] Docker Compose reconstruido
- [ ] Contenedores levantados sin errores
- [ ] DAG visible en Airflow UI
- [ ] Trigger manual ejecutado
- [ ] Logs muestran "✅ Subida a GCS completada"
- [ ] Archivos visibles en GCS bucket
- [ ] No hay errores 401
- [ ] Watermark actualizado

---

**Última actualización**: 2025-12-29 09:35  
**Estado**: ✅ Código modificado, listo para testing
