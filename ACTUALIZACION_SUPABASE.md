# 🎯 ACTUALIZACIÓN: Extracción Real desde Supabase

## ✅ Cambios Implementados

### 📄 Archivos Nuevos Creados

1. **`scripts/extract_from_supabase.py`** (300+ líneas)
   - Script principal de extracción desde Supabase
   - Manejo de watermarks (estado incremental)
   - Extracción incremental (donaciones, gastos)
   - Extracción snapshot (donantes, casos, proveedores)
   - Escritura a Bronze layer en Parquet

2. **`docs/EXTRACCION_SUPABASE.md`**
   - Guía completa de extracción
   - Configuración de credenciales
   - Ejemplos de ejecución
   - Troubleshooting detallado

3. **`docs/QUICKSTART.md`** (actualizado)
   - Guía paso a paso con datos reales
   - Configuración de Supabase
   - Verificación de resultados

### 🔧 Archivos Modificados

1. **`scripts/setup.sh`**
   - Ahora pregunta si usar datos mock o reales
   - Valida credenciales de Supabase
   - Ejecuta extracción automáticamente si está configurado

2. **`.gitignore`**
   - Agregado `watermarks.json` (estado de extracción)

---

## 🔄 Flujo Actualizado

### Antes (Solo Mock)
```
scripts/generate_mock_data.py
    ↓
data/raw/donaciones_mock.parquet
    ↓
jobs/transform_donations.py
```

### Ahora (Real + Mock)
```
┌─────────────────────────────────┐
│  Opción A: Datos REALES         │
│  scripts/extract_from_supabase  │
│         ↓                        │
│  data/raw/donaciones.parquet    │
│  data/raw/gastos.parquet        │
│  data/raw/donantes.parquet      │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│  Opción B: Datos MOCK           │
│  scripts/generate_mock_data.py  │
│         ↓                        │
│  data/raw/donaciones_mock.parquet│
└─────────────────────────────────┘

         ↓ (ambas opciones)
         
jobs/transform_donations.py
    ↓
data/processed/silver/donaciones/
    ↓
data/output/gold/donaciones_monthly/
```

---

## 🚀 Cómo Usar (Paso a Paso)

### 1. Setup Inicial
```bash
cd pyspark-airflow-data-platform
./scripts/setup.sh
```

### 2. Configurar Supabase
```bash
nano .env

# Editar:
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_KEY=***REDACTED***
```

### 3. Extraer Datos Reales
```bash
source venv/bin/activate
python scripts/extract_from_supabase.py
```

### 4. Transformar con PySpark
```bash
spark-submit --master local[*] jobs/transform_donations.py
```

---

## 📊 Características de la Extracción

### ✅ Extracción Incremental (Watermarks)

**Tablas**: `donaciones`, `gastos`

**Cómo funciona**:
1. Lee `watermarks.json` (última fecha procesada)
2. Query: `SELECT * WHERE fecha > watermark`
3. Escribe nuevos datos en modo `append`
4. Actualiza watermark con MAX(fecha)

**Ejemplo**:
```json
// watermarks.json
{
  "donaciones": "2024-12-25",
  "gastos": "2024-12-20"
}
```

**Ventajas**:
- Solo extrae datos nuevos
- Eficiente (no procesa todo cada vez)
- Idempotente (se puede re-ejecutar)

### ✅ Extracción Snapshot (Full Load)

**Tablas**: `donantes`, `casos`, `proveedores`

**Cómo funciona**:
1. Query: `SELECT *` (todos los registros)
2. Escribe en modo `overwrite`
3. No usa watermarks

**Cuándo usar**:
- Tablas maestras (cambian poco)
- Tablas pequeñas
- Necesitas estado completo

---

## 🔍 Validación

### Verificar extracción exitosa:

```bash
# Ver archivos generados
ls -lh data/raw/

# Debería mostrar:
# donaciones.parquet
# gastos.parquet
# donantes.parquet
# casos.parquet
# proveedores.parquet

# Ver watermarks
cat watermarks.json

# Debería mostrar:
# {
#   "donaciones": "2024-12-26",
#   "gastos": "2024-12-25"
# }
```

### Inspeccionar datos:

```python
import pandas as pd

# Leer Parquet
df = pd.read_parquet("data/raw/donaciones.parquet")

# Verificar
print(f"Registros: {len(df)}")
print(f"Columnas: {df.columns.tolist()}")
print(f"Fechas: {df['fecha_donacion'].min()} a {df['fecha_donacion'].max()}")
print(f"Total donado: ${df['monto'].sum():,.2f}")
```

---

## 🎯 Ventajas del Nuevo Sistema

### 1. Datos Reales
- ✅ Conecta directamente a Supabase
- ✅ No necesitas generar datos mock
- ✅ Pruebas con datos de producción

### 2. Incremental
- ✅ Solo extrae datos nuevos
- ✅ Eficiente en tiempo y recursos
- ✅ Watermarks automáticos

### 3. Flexible
- ✅ Puedes usar mock para testing
- ✅ Puedes usar real para desarrollo
- ✅ Mismo código PySpark para ambos

### 4. Mantenible
- ✅ Código reutilizado del CRM original
- ✅ Patrones probados en producción
- ✅ Fácil de extender a más tablas

---

## 📝 Configuración Avanzada

### Agregar más tablas incrementales:

Editar `config/__init__.py`:
```python
INCREMENTAL_TABLES = {
    "donaciones": "fecha_donacion",
    "gastos": "fecha_gasto",
    "adopciones": "fecha_adopcion",  # Nueva
}
```

### Agregar más tablas snapshot:

```python
FULL_LOAD_TABLES = [
    "donantes",
    "casos",
    "proveedores",
    "veterinarias",  # Nueva
]
```

### Resetear watermarks:

```bash
# Opción 1: Eliminar archivo
rm watermarks.json

# Opción 2: Resetear tabla específica
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

## 🔗 Documentación Relacionada

- **Guía de extracción**: [`docs/EXTRACCION_SUPABASE.md`](docs/EXTRACCION_SUPABASE.md)
- **Inicio rápido**: [`docs/QUICKSTART.md`](docs/QUICKSTART.md)
- **Arquitectura**: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## ✅ Checklist de Migración

Si ya tenías el proyecto con datos mock:

- [ ] Ejecutar `./scripts/setup.sh` de nuevo
- [ ] Configurar credenciales en `.env`
- [ ] Ejecutar `python scripts/extract_from_supabase.py`
- [ ] Verificar archivos en `data/raw/`
- [ ] Ejecutar job PySpark
- [ ] Verificar resultados en `data/processed/` y `data/output/`

---

## 🎉 Resultado Final

Ahora tienes:
- ✅ Extracción automática desde Supabase
- ✅ Watermarks para procesamiento incremental
- ✅ Bronze layer con datos reales
- ✅ Transformaciones PySpark funcionando
- ✅ Silver y Gold layers generados
- ✅ Opción de usar mock para testing

**¡El proyecto está listo para trabajar con datos reales!** 🚀

---

**Fecha de actualización**: 2024-12-26  
**Versión**: 2.0.0 (Extracción Real)
