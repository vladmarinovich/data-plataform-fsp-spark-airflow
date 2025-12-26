# ⚡ Inicio Rápido - Extracción Real desde Supabase

## 🎯 Resumen

Esta guía te lleva paso a paso desde cero hasta tener datos **reales** de Supabase procesados con PySpark.

---

## 📋 Paso 1: Setup Inicial

```bash
# Navegar al proyecto
cd pyspark-airflow-data-platform

# Ejecutar setup automatizado
./scripts/setup.sh
```

**El script te preguntará**:
```
¿Quieres usar datos MOCK para testing? (s/n)
```

- **Responde `n`** si quieres usar datos reales de Supabase
- **Responde `s`** si solo quieres probar con datos mock

---

## 🔑 Paso 2: Configurar Credenciales de Supabase

### 2.1 Obtener credenciales

1. Ve a [Supabase Dashboard](https://app.supabase.com)
2. Selecciona tu proyecto
3. Ve a **Settings → API**
4. Copia:
   - **Project URL**: `https://xxxxxxxxxxx.supabase.co`
   - **anon/public key**: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...`

### 2.2 Editar archivo `.env`

```bash
nano .env
```

Reemplaza los valores:
```bash
SUPABASE_URL=https://xxxxxxxxxxx.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Guarda y cierra (`Ctrl+O`, `Enter`, `Ctrl+X`)

---

## 📥 Paso 3: Extraer Datos de Supabase

```bash
# Activar entorno virtual
source venv/bin/activate

# Ejecutar extracción
python scripts/extract_from_supabase.py
```

**Salida esperada**:
```
============================================================
🏁 Iniciando Extracción desde Supabase
============================================================
✅ Conectado a Supabase: https://xxxxx.supabase.co

============================================================
🚀 EXTRACCIÓN INCREMENTAL
============================================================

📥 Extrayendo tabla 'donaciones' (incremental)...
   ✅ Extraídos 1543 registros nuevos
   
📥 Extrayendo tabla 'gastos' (incremental)...
   ✅ Extraídos 892 registros nuevos

============================================================
📸 EXTRACCIÓN SNAPSHOT
============================================================

📸 Extrayendo tabla 'donantes' (snapshot completo)...
   ✅ Extraídos 234 registros totales

============================================================
✅ Extracción completada exitosamente
============================================================
```

### Verificar archivos generados:

```bash
ls -lh data/raw/

# Debería mostrar:
# donaciones.parquet
# gastos.parquet
# donantes.parquet
# casos.parquet
# proveedores.parquet
```

---

## 🚀 Paso 4: Ejecutar Transformación PySpark

```bash
spark-submit --master local[*] jobs/transform_donations.py
```

**Salida esperada**:
```
============================================================
🚀 Iniciando Job: Transform Donations
============================================================

📥 Leyendo datos Bronze (raw)...
📊 DataFrame 'Bronze - Donaciones':
   - Filas: 1,543
   - Columnas: 7

🔄 Transformando a capa Silver (cleaned)...
✅ Tabla 'donaciones': Todas las columnas requeridas presentes
✅ Columna 'fecha_donacion': Todas las fechas válidas
✅ Columna 'monto': Todos los montos positivos

💾 Escribiendo datos Silver...
✅ Datos Silver escritos en: data/processed/silver/donaciones

📊 Transformando a capa Gold (aggregated)...
💾 Escribiendo datos Gold...
✅ Datos Gold escritos en: data/output/gold/donaciones_monthly

============================================================
✅ Job completado exitosamente
============================================================
```

---

## ✅ Paso 5: Verificar Resultados

### Ver datos Silver (particionados):

```bash
ls -R data/processed/silver/donaciones/

# Debería mostrar estructura:
# data/processed/silver/donaciones/:
# _SUCCESS  anio=2023  anio=2024
#
# data/processed/silver/donaciones/anio=2023:
# mes=01  mes=02  mes=03  ...
```

### Ver datos Gold (agregados):

```bash
ls -lh data/output/gold/donaciones_monthly/

# Debería mostrar archivos Parquet
```

### Inspeccionar con PySpark:

```bash
pyspark
```

```python
# Leer datos Silver
df_silver = spark.read.parquet("data/processed/silver/donaciones")
df_silver.printSchema()
df_silver.show(5)

# Ver totales por mes
df_silver.groupBy("anio", "mes").count().orderBy("anio", "mes").show()

# Leer datos Gold
df_gold = spark.read.parquet("data/output/gold/donaciones_monthly")
df_gold.show()
```

---

## 🔄 Ejecuciones Posteriores

### Extraer solo datos nuevos:

```bash
# Activar venv
source venv/bin/activate

# Extraer (solo trae registros nuevos)
python scripts/extract_from_supabase.py

# Transformar
spark-submit --master local[*] jobs/transform_donations.py
```

**Gracias a los watermarks**, solo se extraerán registros con fechas posteriores a la última extracción.

---

## 🐛 Troubleshooting

### Error: "Faltan credenciales de Supabase"

```bash
# Verificar .env
cat .env | grep SUPABASE

# Debería mostrar tus credenciales
```

### Error: "Connection refused"

- Verifica que la URL sea correcta
- Verifica que el API Key sea el correcto
- Verifica que el proyecto no esté pausado en Supabase

### No se extraen datos

```bash
# Ver watermarks actuales
cat watermarks.json

# Si están adelantados, resetear:
rm watermarks.json
```

### Error en transformación PySpark

```bash
# Verificar que existan los archivos de entrada
ls -lh data/raw/donaciones.parquet

# Si no existe, ejecutar extracción primero
python scripts/extract_from_supabase.py
```

---

## 📚 Documentación Adicional

- **Extracción detallada**: [`docs/EXTRACCION_SUPABASE.md`](EXTRACCION_SUPABASE.md)
- **Arquitectura**: [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)
- **README principal**: [`README.md`](../README.md)

---

## 🎯 Próximos Pasos

1. ✅ Configurar Airflow para automatizar
2. ✅ Agregar más transformaciones (gastos, casos, etc.)
3. ✅ Configurar deployment a cloud (AWS/GCP/Azure)
4. ✅ Agregar monitoreo y alertas

---

**¡Listo! Ahora tienes un pipeline funcional con datos reales de Supabase** 🚀
