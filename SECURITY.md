# 🔒 Guía de Seguridad - Airflow Data Platform

## ⚠️ IMPORTANTE: Credenciales y Secretos

Este proyecto maneja información sensible. **NUNCA** subas credenciales a Git.

### Archivos Sensibles (Ya están en .gitignore)

```
✅ .env                          # Variables de entorno
✅ config/*.json                 # Credenciales de GCP
✅ *.key, *.pem                  # Claves privadas
✅ watermarks.json               # Estado del pipeline
✅ data/                         # Datos del data lake
```

## 📋 Configuración Inicial

### 1. Copiar archivo de ejemplo

```bash
cp .env.example .env
```

### 2. Generar claves de seguridad

```bash
# Fernet Key para Airflow
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret Key para Flask
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 3. Configurar credenciales de GCP

1. Descargar service account key desde GCP Console
2. Guardar como `config/gcp-service-account.json`
3. **NUNCA** commitear este archivo

### 4. Configurar Supabase

1. Obtener URL y Anon Key desde Supabase Dashboard
2. Agregar a `.env`:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your-anon-key-here
   ```

## 🐳 Docker Compose - Secrets

El `docker-compose.yaml` usa variables de entorno del archivo `.env`:

```yaml
environment:
  - SUPABASE_URL=${SUPABASE_URL}
  - SUPABASE_KEY=${SUPABASE_KEY}
  - GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-service-account.json
```

## 🚨 Checklist de Seguridad

Antes de hacer commit:

- [ ] `.env` está en `.gitignore`
- [ ] No hay credenciales hardcodeadas en el código
- [ ] `config/gcp-service-account.json` está en `.gitignore`
- [ ] Variables sensibles usan `os.getenv()` o archivos `.env`
- [ ] Watermarks y datos no están en Git

## 🔐 Rotación de Credenciales

Si una credencial se expone:

1. **Inmediatamente** revocar en la consola correspondiente (GCP/Supabase)
2. Generar nuevas credenciales
3. Actualizar `.env` y secretos de Docker
4. Reiniciar servicios

## 📚 Referencias

- [Airflow Security Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html)
- [GCP Service Account Best Practices](https://cloud.google.com/iam/docs/best-practices-for-managing-service-account-keys)
- [Supabase Security](https://supabase.com/docs/guides/platform/going-into-prod#security)
