# 🚀 Guía de Despliegue en Producción (GCP VM)

**Objetivo**: Desplegar Airflow en una VM de Google Compute Engine con autenticación mediante Service Account attached.

---

## 🏗️ ARQUITECTURA DE PRODUCCIÓN

```
┌──────────────────────────────────────────────────────────┐
│ Compute Engine VM                                        │
├──────────────────────────────────────────────────────────┤
│ • Ubuntu 22.04 LTS                                       │
│ • Docker + Docker Compose                                │
│ • Service Account: airflow-production@...               │
│ • Credenciales: GCE Metadata Server (automático)         │
│ • NO necesita archivos JSON                              │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│ Airflow Containers                                       │
├──────────────────────────────────────────────────────────┤
│ • airflow-scheduler                                      │
│ • airflow-webserver                                      │
│ • postgres (metadata)                                    │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│ GCP Services                                             │
├──────────────────────────────────────────────────────────┤
│ • Cloud Storage (Data Lake)                              │
│ • BigQuery (Analytics)                                   │
│ • Supabase (Source DB)                                   │
└──────────────────────────────────────────────────────────┘
```

---

## 📋 PREREQUISITOS

### 1. Service Account Creada

Ya deberías tener:
- ✅ Service Account: `airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com`
- ✅ Roles asignados:
  - Storage Object Admin
  - BigQuery Data Editor
  - BigQuery Job User

### 2. Desbloquear Política (Si aún no lo hiciste)

1. Ve a: [Organization Policies](https://console.cloud.google.com/iam-admin/orgpolicies?project=salvando-patitas-de-spark)
2. Busca: **"Disable service account key creation"**
3. Cambia a: **"Not enforced"**
4. **Save**

---

## 🖥️ PASO 1: Crear VM en GCP

### Opción A: Desde la Consola (UI)

1. Ve a: [Compute Engine → VM Instances](https://console.cloud.google.com/compute/instances?project=salvando-patitas-de-spark)

2. Click **"CREATE INSTANCE"**

3. **Configuración**:
   ```
   Name: airflow-production
   Region: us-central1 (o el más cercano a ti)
   Zone: us-central1-a
   
   Machine type: e2-medium (2 vCPU, 4 GB RAM)
   Boot disk: Ubuntu 22.04 LTS, 50 GB SSD
   
   Identity and API access:
   ├─ Service account: airflow-production@...
   └─ Access scopes: "Allow full access to all Cloud APIs"
   
   Firewall:
   ├─ ✅ Allow HTTP traffic
   └─ ✅ Allow HTTPS traffic
   ```

4. Click **"CREATE"**

### Opción B: Desde CLI (gcloud)

```bash
gcloud compute instances create airflow-production \
  --project=salvando-patitas-de-spark \
  --zone=us-central1-a \
  --machine-type=e2-medium \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-ssd \
  --service-account=airflow-production@salvando-patitas-de-spark.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --tags=http-server,https-server
```

---

## 🔧 PASO 2: Configurar la VM

### Conectarse a la VM

```bash
gcloud compute ssh airflow-production --zone=us-central1-a
```

### Instalar Docker

```bash
# Actualizar sistema
sudo apt-get update
sudo apt-get upgrade -y

# Instalar Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# Instalar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verificar instalación
docker --version
docker-compose --version

# Logout y login para aplicar cambios de grupo
exit
```

### Volver a conectar

```bash
gcloud compute ssh airflow-production --zone=us-central1-a
```

---

## 📦 PASO 3: Clonar el Proyecto

```bash
# Instalar Git
sudo apt-get install -y git

# Clonar repositorio (ajusta la URL)
git clone https://github.com/tu-usuario/pyspark-airflow-data-platform.git
cd pyspark-airflow-data-platform

# O si usas Google Drive, sube el código manualmente
```

---

## 🔐 PASO 4: Configurar Variables de Entorno

```bash
# Crear archivo .env
cp .env.example .env

# Editar con tus credenciales
nano .env
```

**Contenido del `.env`**:
```bash
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key-here

# Entorno
ENV=cloud

# GCP (NO necesitas GOOGLE_APPLICATION_CREDENTIALS en VM)
# La VM usa automáticamente el service account attached
```

---

## 🐳 PASO 5: Actualizar Docker Compose para Producción

Crea un archivo `docker-compose.prod.yaml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow_prod_2025}
      - POSTGRES_DB=airflow
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U airflow" ]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    restart: always

  airflow-setup:
    build: .
    depends_on:
      - postgres
    volumes:
      - .:/opt/airflow
    environment:
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${POSTGRES_PASSWORD:-airflow_prod_2025}@postgres/airflow
      - SUPABASE_URL=${SUPABASE_URL}
      - SUPABASE_KEY=${SUPABASE_KEY}
      - ENV=cloud
    entrypoint: >
      bash -c "airflow db init && airflow users create --username admin --firstname Admin --lastname Airflow --role Admin --email admin@spdp.com --password ${AIRFLOW_ADMIN_PASSWORD:-admin} || true"

  airflow-scheduler:
    build: .
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - .:/opt/airflow
      - airflow-logs:/opt/airflow/logs
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${POSTGRES_PASSWORD:-airflow_prod_2025}@postgres/airflow
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - SUPABASE_URL=${SUPABASE_URL}
      - SUPABASE_KEY=${SUPABASE_KEY}
      - ENV=cloud
    command: scheduler

  airflow-webserver:
    build: .
    restart: always
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - .:/opt/airflow
      - airflow-logs:/opt/airflow/logs
    ports:
      - "80:8080"  # Exponer en puerto 80
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${POSTGRES_PASSWORD:-airflow_prod_2025}@postgres/airflow
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_SECRET_KEY:-change-this-in-production}
      - AIRFLOW__WEBSERVER__WORKERS=2
      - AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300
      - AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
      - SUPABASE_URL=${SUPABASE_URL}
      - SUPABASE_KEY=${SUPABASE_KEY}
      - ENV=cloud
    command: webserver

volumes:
  postgres-db-volume:
  airflow-logs:
```

**Diferencias clave con desarrollo**:
- ❌ NO monta `~/.config/gcloud` (usa metadata server)
- ❌ NO define `GOOGLE_APPLICATION_CREDENTIALS`
- ✅ Puerto 80 (en lugar de 8080)
- ✅ Volúmenes persistentes para logs
- ✅ Contraseñas desde variables de entorno

---

## 🚀 PASO 6: Levantar Airflow

```bash
# Construir imágenes
docker-compose -f docker-compose.prod.yaml build

# Levantar servicios
docker-compose -f docker-compose.prod.yaml up -d

# Ver logs
docker-compose -f docker-compose.prod.yaml logs -f

# Verificar estado
docker-compose -f docker-compose.prod.yaml ps
```

---

## 🔍 PASO 7: Verificar Autenticación

```bash
# Entrar al contenedor del scheduler
docker-compose -f docker-compose.prod.yaml exec airflow-scheduler bash

# Verificar credenciales
python3 /opt/airflow/config/credentials.py

# Deberías ver:
# 🔐 Autenticación: GCE Metadata Server (VM)
# ✅ Proyecto GCP: salvando-patitas-de-spark
# ✅ Credenciales válidas y funcionando
```

---

## 🌐 PASO 8: Configurar Firewall

### Crear regla de firewall para Airflow UI

```bash
gcloud compute firewall-rules create allow-airflow \
  --project=salvando-patitas-de-spark \
  --direction=INGRESS \
  --priority=1000 \
  --network=default \
  --action=ALLOW \
  --rules=tcp:80 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=http-server
```

### Obtener IP externa

```bash
gcloud compute instances describe airflow-production \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### Acceder a Airflow UI

```
http://[IP-EXTERNA]
Usuario: admin
Password: (el que configuraste en .env)
```

---

## 🔐 SEGURIDAD EN PRODUCCIÓN

### 1. Configurar HTTPS (Recomendado)

```bash
# Instalar Nginx + Certbot
sudo apt-get install -y nginx certbot python3-certbot-nginx

# Configurar dominio (ej: airflow.tudominio.com)
# Obtener certificado SSL
sudo certbot --nginx -d airflow.tudominio.com
```

### 2. Restringir Acceso por IP

```bash
# Solo permitir tu IP
gcloud compute firewall-rules update allow-airflow \
  --source-ranges=TU.IP.PUBLICA.AQUI/32
```

### 3. Cambiar Contraseñas por Defecto

En `.env`:
```bash
POSTGRES_PASSWORD=$(openssl rand -base64 32)
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 16)
AIRFLOW_SECRET_KEY=$(openssl rand -hex 32)
```

---

## 📊 MONITOREO

### Ver logs en tiempo real

```bash
# Scheduler
docker-compose -f docker-compose.prod.yaml logs -f airflow-scheduler

# Webserver
docker-compose -f docker-compose.prod.yaml logs -f airflow-webserver

# Todos
docker-compose -f docker-compose.prod.yaml logs -f
```

### Verificar uso de recursos

```bash
docker stats
```

---

## 🔄 ACTUALIZACIÓN DEL CÓDIGO

```bash
# SSH a la VM
gcloud compute ssh airflow-production --zone=us-central1-a

# Ir al directorio
cd pyspark-airflow-data-platform

# Pull cambios
git pull

# Reconstruir y reiniciar
docker-compose -f docker-compose.prod.yaml down
docker-compose -f docker-compose.prod.yaml build --no-cache
docker-compose -f docker-compose.prod.yaml up -d
```

---

## ✅ CHECKLIST DE DESPLIEGUE

- [ ] VM creada con service account attached
- [ ] Docker y Docker Compose instalados
- [ ] Código clonado en la VM
- [ ] `.env` configurado con credenciales
- [ ] `docker-compose.prod.yaml` creado
- [ ] Contenedores levantados sin errores
- [ ] Verificación de credenciales exitosa
- [ ] Firewall configurado
- [ ] Airflow UI accesible desde navegador
- [ ] DAG visible y ejecutable
- [ ] Logs muestran "GCE Metadata Server"
- [ ] Datos suben correctamente a GCS
- [ ] HTTPS configurado (opcional pero recomendado)

---

## 🆚 COMPARACIÓN: Dev vs Prod

| Aspecto | Desarrollo (Local) | Producción (VM) |
|---------|-------------------|-----------------|
| **Credenciales** | ADC (tu cuenta) | Service Account (VM) |
| **Archivo JSON** | Montado desde host | NO necesario |
| **Autenticación** | `~/.config/gcloud/` | Metadata Server |
| **Puerto** | 8080 | 80 (HTTP) o 443 (HTTPS) |
| **Persistencia** | Temporal | Volúmenes Docker |
| **Seguridad** | Desarrollo | Firewall + HTTPS |
| **Logs** | Muestra tu email | Muestra service account |

---

**Última actualización**: 2025-12-29  
**Estado**: Listo para despliegue
