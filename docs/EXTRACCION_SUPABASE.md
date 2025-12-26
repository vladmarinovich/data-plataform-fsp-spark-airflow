# 🔄 Guía de Extracción desde Supabase

## 📋 Resumen

Este documento explica cómo extraer datos **reales** desde Supabase en lugar de usar datos mock.

---

## 🎯 Diferencias: Mock vs Real

### Flujo Anterior (Mock)
```
scripts/generate_mock_data.py
    ↓
data/raw/donaciones_mock.parquet
    ↓
jobs/transform_donations.py
```

### Flujo Nuevo (Real)
```
scripts/extract_from_supabase.py
    ↓
data/raw/donaciones.parquet
data/raw/gastos.parquet
data/raw/donantes.parquet
    ↓
jobs/transform_donations.py
```

---

## ⚙️ Configuración Inicial

### 1. Editar archivo `.env`

```bash
# Abrir archivo .env
nano .env
```

Configurar credenciales de Supabase:
```bash
SUPABASE_URL=https://xxxxxxxxxxx.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**¿Dónde encontrar estas credenciales?**
1. Ir a [Supabase Dashboard](https://app.supabase.com)
2. Seleccionar tu proyecto
3. Settings → API
4. Copiar:
   - **URL**: Project URL
   - **Key**: `anon` `public` key

### 2. Verificar conexión

```bash
# Activar entorno virtual
source venv/bin/activate

# Probar conexión
python -c "
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()
client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
print('✅ Conexión exitosa a Supabase')
"
```

---

## 🚀 Extracción de Datos

### Ejecución Manual

```bash
# Activar entorno virtual
source venv/bin/activate

# Ejecutar extracción
python scripts/extract_from_supabase.py
```

### Qué hace el script

#### 1. Carga Watermarks
```
📂 Busca archivo: watermarks.json
   Si existe: Lee última fecha procesada por tabla
   Si no existe: Usa fecha default "2020-01-01"
```

#### 2. Extracción Incremental
```
Para cada tabla incremental (donaciones, gastos):
  1. Lee watermark actual
  2. Query: SELECT * WHERE fecha > watermark
  3. Guarda en data/raw/{tabla}.parquet (modo append)
  4. Calcula nuevo watermark (MAX fecha)
  5. Actualiza watermarks.json
```

#### 3. Extracción Snapshot
```
Para cada tabla snapshot (donantes, casos, proveedores):
  1. Query: SELECT * (todos los registros)
  2. Guarda en data/raw/{tabla}.parquet (modo overwrite)
```

---

## 📊 Ejemplo de Ejecución

### Primera Ejecución (Sin watermarks)

```bash
$ python scripts/extract_from_supabase.py

============================================================
🏁 Iniciando Extracción desde Supabase
   Timestamp: 2024-12-26 10:30:00
============================================================
✅ Conectado a Supabase: https://xxxxx.supabase.co
⚠️  No existe archivo de watermarks, creando nuevo...

============================================================
🚀 EXTRACCIÓN INCREMENTAL
============================================================

📥 Extrayendo tabla 'donaciones' (incremental)...
   Watermark actual: 2020-01-01
   ✅ Extraídos 1543 registros nuevos
   Rango de fechas: 2023-01-15 a 2024-12-25

💾 Escribiendo a Bronze layer: data/raw/donaciones.parquet
   Modo: append
   ✅ Escritura completa: 1543 registros
   📊 Nuevo watermark: 2024-12-25

📥 Extrayendo tabla 'gastos' (incremental)...
   Watermark actual: 2020-01-01
   ✅ Extraídos 892 registros nuevos
   Rango de fechas: 2023-02-10 a 2024-12-20

💾 Escribiendo a Bronze layer: data/raw/gastos.parquet
   Modo: append
   ✅ Escritura completa: 892 registros
   📊 Nuevo watermark: 2024-12-20

============================================================
📸 EXTRACCIÓN SNAPSHOT
============================================================

📸 Extrayendo tabla 'donantes' (snapshot completo)...
   ✅ Extraídos 234 registros totales

💾 Escribiendo a Bronze layer: data/raw/donantes.parquet
   Modo: overwrite
   ✅ Escritura completa: 234 registros

✅ Watermarks guardados: {
  "donaciones": "2024-12-25",
  "gastos": "2024-12-20"
}

============================================================
✅ Extracción completada exitosamente
============================================================

💡 Siguiente paso:
   spark-submit --master local[*] jobs/transform_donations.py
```

### Segunda Ejecución (Con watermarks)

```bash
$ python scripts/extract_from_supabase.py

============================================================
🏁 Iniciando Extracción desde Supabase
============================================================
✅ Conectado a Supabase: https://xxxxx.supabase.co
✅ Watermarks cargados: {
  "donaciones": "2024-12-25",
  "gastos": "2024-12-20"
}

============================================================
🚀 EXTRACCIÓN INCREMENTAL
============================================================

📥 Extrayendo tabla 'donaciones' (incremental)...
   Watermark actual: 2024-12-25
   ✅ Extraídos 5 registros nuevos
   Rango de fechas: 2024-12-26 a 2024-12-26

💾 Escribiendo a Bronze layer: data/raw/donaciones.parquet
   Modo: append
   ✅ Append exitoso: +5 registros nuevos
   Total en Bronze: 1548 registros
   📊 Nuevo watermark: 2024-12-26

📥 Extrayendo tabla 'gastos' (incremental)...
   Watermark actual: 2024-12-20
   ⚠️  No hay datos nuevos

============================================================
✅ Extracción completada exitosamente
============================================================
```

---

## 📁 Archivos Generados

### Estructura después de extracción:

```
data/raw/
├── donaciones.parquet      # Datos incrementales (append)
├── gastos.parquet          # Datos incrementales (append)
├── donantes.parquet        # Snapshot (overwrite)
├── casos.parquet           # Snapshot (overwrite)
└── proveedores.parquet     # Snapshot (overwrite)

watermarks.json             # Estado de última extracción
```

### Contenido de `watermarks.json`:

```json
{
  "donaciones": "2024-12-26",
  "gastos": "2024-12-20"
}
```

---

## 🔧 Configuración Avanzada

### Agregar más tablas

Editar `config/__init__.py`:

```python
# Tablas incrementales (append-only)
INCREMENTAL_TABLES = {
    "donaciones": "fecha_donacion",
    "gastos": "fecha_gasto",
    "adopciones": "fecha_adopcion",  # Nueva tabla
}

# Tablas snapshot (full overwrite)
FULL_LOAD_TABLES = [
    "donantes",
    "casos",
    "proveedores",
    "veterinarias",  # Nueva tabla
]
```

### Resetear watermarks

```bash
# Opción 1: Eliminar archivo
rm watermarks.json

# Opción 2: Editar manualmente
nano watermarks.json
# Cambiar fechas a "2020-01-01"

# Opción 3: Resetear una tabla específica
python -c "
import json
with open('watermarks.json', 'r') as f:
    w = json.load(f)
w['donaciones'] = '2020-01-01'
with open('watermarks.json', 'w') as f:
    json.dump(w, f, indent=2)
"
```

---

## 🐛 Troubleshooting

### Error: "Faltan credenciales de Supabase"

**Solución**:
```bash
# Verificar que .env tenga las variables
cat .env | grep SUPABASE

# Debería mostrar:
# SUPABASE_URL=https://...
# SUPABASE_KEY=eyJ...
```

### Error: "Connection refused" o "Unauthorized"

**Causas posibles**:
1. URL incorrecta
2. API Key incorrecta
3. Proyecto pausado en Supabase
4. Firewall bloqueando conexión

**Solución**:
```bash
# Verificar URL y Key en Supabase Dashboard
# Settings → API → Project URL y anon/public key
```

### Error: "Table does not exist"

**Causa**: La tabla no existe en Supabase

**Solución**:
```bash
# Verificar tablas disponibles
python -c "
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()
client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Listar tablas (requiere permisos)
response = client.table('donaciones').select('*').limit(1).execute()
print('✅ Tabla donaciones existe')
"
```

### No se extraen datos nuevos

**Causa**: Watermark está adelantado

**Solución**:
```bash
# Ver watermarks actuales
cat watermarks.json

# Resetear si es necesario
rm watermarks.json
```

---

## 🔄 Integración con PySpark

### Flujo completo:

```bash
# 1. Extraer datos de Supabase
python scripts/extract_from_supabase.py

# 2. Transformar con PySpark
spark-submit --master local[*] jobs/transform_donations.py

# 3. Verificar resultados
ls -lh data/processed/silver/donaciones/
ls -lh data/output/gold/donaciones_monthly/
```

### Automatización con cron:

```bash
# Editar crontab
crontab -e

# Agregar ejecución diaria a las 2 AM
0 2 * * * cd /path/to/project && source venv/bin/activate && python scripts/extract_from_supabase.py && spark-submit --master local[*] jobs/transform_donations.py
```

---

## 📝 Mejores Prácticas

### 1. Backup de watermarks
```bash
# Antes de resetear, hacer backup
cp watermarks.json watermarks.backup.json
```

### 2. Logs de extracción
```bash
# Guardar logs
python scripts/extract_from_supabase.py 2>&1 | tee logs/extract_$(date +%Y%m%d_%H%M%S).log
```

### 3. Validar datos extraídos
```python
import pandas as pd

# Leer y validar
df = pd.read_parquet("data/raw/donaciones.parquet")
print(f"Registros: {len(df)}")
print(f"Rango fechas: {df['fecha_donacion'].min()} a {df['fecha_donacion'].max()}")
print(f"Nulos: {df.isnull().sum()}")
```

---

## 🎯 Próximos Pasos

1. ✅ Configurar credenciales en `.env`
2. ✅ Ejecutar primera extracción
3. ✅ Verificar archivos en `data/raw/`
4. ✅ Ejecutar transformación PySpark
5. 🔄 Configurar Airflow para automatizar

---

**Documentación actualizada**: 2024-12-26
