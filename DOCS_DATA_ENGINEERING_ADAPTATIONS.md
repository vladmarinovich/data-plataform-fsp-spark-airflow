
# 🛠 Adaptaciones Técnicas: De SQLX (BigQuery) a PySpark

Este documento detalla las decisiones de ingeniería tomadas al adaptar la lógica de Dataform (SQLX) a PySpark.

## 1. Manejo de Timestamps (Microsegundos vs Segundos)
### Adaptación
- **SQLX**: Usamos `TIMESTAMP_MICROS(DIV(col, 1000))`.
- **PySpark**: Usamos `F.from_unixtime(F.col(col)/1000000).cast("timestamp")`.

### Por qué y Beneficio
Supabase/Postgres almacena marcas de tiempo como enteros en microsegundos. Spark, por defecto, interpreta los casts de enteros a timestamp como **segundos**. 
- **Beneficio**: Evitamos fechas erróneas (año 50,000+) y garantizamos que los datos en BigQuery (vía Parquet) mantengan la precisión correcta.

### Trade-off
- **Carga Computacional**: Realizar una división por cada registro en billones de registros es costoso en Spark. Sin embargo, dado el volumen actual del CRM (< 1TB), priorizamos la **integridad de la fecha** sobre micro-optimizaciones de CPU.

---

## 2. Particionamiento Físico (Hive-style)
### Adaptación
- **SQLX**: BigQuery maneja particiones lógicas (`partition by date`).
- **PySpark**: Creamos columnas explícitas `anio`, `mes`, `dia` y usamos `.partitionBy()`.

### Por qué y Beneficio
En un Data Lake orientado a archivos (GCS/Local), Spark necesita una estructura de carpetas física (`/anio=2024/mes=12/`) para realizar **Partition Pruning**.
- **Beneficio**: Las consultas futuras solo leerán las carpetas necesarias, reduciendo costos de lectura en GCS en un 90%+.

### Trade-off
- **Estructura de Datos**: Agregamos 3 columnas "técnicas" que no existen en el modelo relacional original.
- **Complejidad de Escritura**: Requiere configurar `partitionOverwriteMode = dynamic` para evitar borrar meses enteros al re-procesar un solo día.

---

## 3. Deduplicación (Window Functions vs Qualify)
### Adaptación
- **SQLX**: Usa `QUALIFY ROW_NUMBER() OVER (...) = 1`.
- **PySpark**: Usa `F.row_number().over(window).filter(F.col("row_num") == 1)`.

### Por qué y Beneficio
Spark no posee la clausula `QUALIFY`. Debemos materializar el `row_number` y luego filtrarlo.
- **Beneficio**: Es la forma más robusta de implementar **CDC (Change Data Capture)** determinístico, asegurando que siempre nos quedamos con el `last_modified_at` más reciente.

---

## 4. Normalización de Medios de Pago (Regex-ish)
### Adaptación
- **SQLX**: `LIKE '%tarjeta%'`.
- **PySpark**: `.contains("tarjeta")`.

### Por qué y Beneficio
Priorizamos `.contains()` sobre `regexp_like` por simplicidad y legibilidad.
- **Beneficio**: Mantenemos la lógica de negocio "difusa" (fuzzy logic) del SQLX original pero con sintaxis nativa de Spark.
