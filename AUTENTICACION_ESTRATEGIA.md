# 🎯 Estrategia de Autenticación: Dev vs Prod

## 📊 RESUMEN VISUAL

```
┌─────────────────────────────────────────────────────────────┐
│ DESARROLLO LOCAL (Tu Laptop)                               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Tu Máquina                                                 │
│  ├─ gcloud auth application-default login                  │
│  └─ ~/.config/gcloud/application_default_credentials.json  │
│                                                             │
│           ↓ (montado en Docker)                             │
│                                                             │
│  Docker Container                                           │
│  ├─ Volumen: ~/.config/gcloud → /root/.config/gcloud       │
│  ├─ ENV: GOOGLE_APPLICATION_CREDENTIALS=/root/.config/...  │
│  └─ Python SDK lee el archivo JSON                         │
│                                                             │
│  ✅ Autenticación: ADC User Credentials                    │
│  ❌ NO necesita: Service Account Key                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘

                            VS

┌─────────────────────────────────────────────────────────────┐
│ PRODUCCIÓN (GCP Compute Engine VM)                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  VM Configuration (al crear la VM)                          │
│  ├─ Service Account: airflow-production@...                │
│  └─ Access Scopes: "Allow full access to all Cloud APIs"   │
│                                                             │
│           ↓ (automático, sin archivos)                      │
│                                                             │
│  GCE Metadata Server                                        │
│  ├─ Endpoint: http://metadata.google.internal/...          │
│  ├─ Provee: Access tokens temporales                       │
│  └─ Renovación: Automática cada ~1 hora                    │
│                                                             │
│           ↓ (Python SDK lo detecta solo)                    │
│                                                             │
│  Docker Container                                           │
│  ├─ NO monta archivos de credenciales                      │
│  ├─ NO define GOOGLE_APPLICATION_CREDENTIALS               │
│  └─ Python SDK usa metadata server automáticamente         │
│                                                             │
│  ✅ Autenticación: GCE Metadata Server                     │
│  ❌ NO necesita: Archivos JSON                             │
│  ❌ NO necesita: Service Account Key descargable           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 DIFERENCIA CLAVE: Service Account vs Service Account Key

### **Service Account** (Lo que SÍ necesitas)
```
✅ Crear en: IAM & Admin → Service Accounts
✅ Nombre: airflow-production
✅ Roles: Storage Object Admin, BigQuery Data Editor, BigQuery Job User
✅ Usar para: Asignar a la VM (attach)
❌ NO descargar: Key JSON
```

**Cómo se usa**:
1. Creas el service account en GCP Console
2. Le das permisos (roles)
3. Al crear la VM, seleccionas este service account
4. La VM automáticamente tiene acceso
5. **NO hay archivos JSON involucrados**

### **Service Account Key** (Lo que NO necesitas en VM)
```
❌ Crear en: Service Account → Keys → Add Key
❌ Formato: JSON file
❌ Contiene: private_key (permanente)
❌ Usar para: Aplicaciones fuera de GCP (on-premise, otras clouds)
✅ Bloqueado por: Tu política organizacional
```

**Cuándo se usa**:
- Aplicaciones corriendo fuera de GCP
- CI/CD en GitHub Actions, GitLab CI
- Servidores on-premise
- **NO para VMs de GCP** (usan metadata server)

---

## 🚀 FLUJO COMPLETO EN PRODUCCIÓN (Sin Keys)

### Paso 1: Crear Service Account (Sin descargar key)

```bash
# Desde gcloud CLI (o desde UI)
gcloud iam service-accounts create airflow-production \
  --display-name="Airflow Production Service Account" \
  --project=salvando-patitas-de-spark

# Asignar roles
gcloud projects add-iam-policy-binding salvando-patitas-de-spark \
  --member="serviceAccount:airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding salvando-patitas-de-spark \
  --member="serviceAccount:airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding salvando-patitas-de-spark \
  --member="serviceAccount:airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

### Paso 2: Crear VM con Service Account Attached

```bash
gcloud compute instances create airflow-production \
  --project=salvando-patitas-de-spark \
  --zone=us-central1-a \
  --machine-type=e2-medium \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --service-account=airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform
  
# ↑ Nota: --service-account (NO --key-file)
```

### Paso 3: En la VM, Python SDK Detecta Automáticamente

```python
# En tu código (config/credentials.py)
from google.auth import default

credentials, project = default()
# ↑ Esto automáticamente:
# 1. Detecta que está en una VM de GCP
# 2. Llama al metadata server
# 3. Obtiene access token del service account
# 4. ¡Listo! Sin archivos JSON
```

---

## 📋 CHECKLIST: ¿Necesito Service Account Key?

| Escenario | ¿Necesito Key JSON? |
|-----------|---------------------|
| Desarrollo local con Docker | ❌ NO (usa ADC) |
| VM de Compute Engine | ❌ NO (usa metadata server) |
| Cloud Run | ❌ NO (usa metadata server) |
| Google Kubernetes Engine (GKE) | ❌ NO (usa Workload Identity) |
| Cloud Functions | ❌ NO (usa metadata server) |
| App Engine | ❌ NO (usa metadata server) |
| GitHub Actions (fuera de GCP) | ✅ SÍ (necesita key) |
| Servidor on-premise | ✅ SÍ (necesita key) |
| AWS EC2 / Azure VM | ✅ SÍ (necesita key) |

---

## 🎯 TU CASO ESPECÍFICO

### Desarrollo (Ahora)
```yaml
# docker-compose.yaml
volumes:
  - ~/.config/gcloud:/root/.config/gcloud:ro  # ← ADC de tu cuenta

environment:
  - GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json
```

**Resultado**: 
- ✅ Usa tus credenciales personales
- ✅ Logs muestran tu email
- ✅ Rápido para iterar

### Producción (Futuro)
```yaml
# docker-compose.prod.yaml
# ❌ NO volumes de credenciales
# ❌ NO GOOGLE_APPLICATION_CREDENTIALS

environment:
  - ENV=cloud  # ← Solo esto
```

**Resultado**:
- ✅ Usa service account de la VM
- ✅ Logs muestran "airflow-production@..."
- ✅ Más seguro (sin archivos)
- ✅ Tokens temporales auto-renovables

---

## 🔐 VENTAJAS DE NO USAR KEYS

1. **Seguridad**:
   - ❌ No hay archivos JSON que robar
   - ❌ No hay private keys permanentes
   - ✅ Tokens expiran cada hora

2. **Simplicidad**:
   - ❌ No hay archivos que gestionar
   - ❌ No hay rotación manual de keys
   - ✅ Todo automático

3. **Auditoría**:
   - ✅ Logs claros de qué VM hizo qué
   - ✅ Fácil revocar acceso (cambiar service account de la VM)

4. **Cumplimiento**:
   - ✅ Cumple con políticas de seguridad
   - ✅ Best practice de Google Cloud

---

## ✅ RESUMEN EJECUTIVO

**Para Desarrollo Local**:
- Usa: ADC (`gcloud auth application-default login`)
- Monta: `~/.config/gcloud` en Docker
- Código: `config/credentials.py` detecta automáticamente

**Para Producción (VM)**:
- Crea: Service account (sin descargar key)
- Asigna: Service account a la VM al crearla
- Código: **El mismo** `config/credentials.py` detecta automáticamente
- Resultado: Metadata server provee credenciales

**NO necesitas**:
- ❌ Descargar service account keys
- ❌ Gestionar archivos JSON en producción
- ❌ Configurar GOOGLE_APPLICATION_CREDENTIALS en la VM
- ❌ Desbloquear la política de keys (puedes dejarla bloqueada)

---

**Conclusión**: La política que bloquea keys es **perfecta** para tu caso. No la desbloquees. 🎯
