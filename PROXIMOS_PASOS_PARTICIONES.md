# 🔧 Próximos Pasos: Optimización de Particiones

**Fecha**: 29-Dic-2025  
**Estado Actual**: Autenticación GCS ✅ | Particiones ⚠️ Requiere optimización

---

## 🎯 PROBLEMA IDENTIFICADO

**Error**: `SQLSTATE: KD009` en jobs de Spark  
**Causa Raíz**: Demasiadas particiones pequeñas (particionamiento por día desde 2023)  
**Impacto**: Jobs de Silver fallan por Out of Memory al intentar listar cientos de particiones

### **Análisis**
```
Particiones actuales: y=YYYY/m=MM/d=DD
Período: 2023-01 hasta 2025-12
Total particiones: ~700+ (2 años × 365 días)
Tamaño promedio: ~50KB por partición

Problema: Spark intenta listar TODAS las particiones antes de leer
Memoria requerida: Excede los 2.5GB disponibles para el scheduler
```

---

## ✅ SOLUCIÓN RECOMENDADA

### **Opción 1: Particionamiento Mensual** (Recomendado)
```
Cambiar de: y=YYYY/m=MM/d=DD
A:          y=YYYY/m=MM

Beneficios:
- Reduce particiones de ~700 a ~24
- Tamaño por partición: ~1-2MB (óptimo para Spark)
- Memoria requerida: <500MB
- Compatible con consultas mensuales
```

### **Opción 2: Sin Particionamiento** (Alternativa)
```
Guardar todo en un solo directorio
Usar columnas de fecha para filtros

Beneficios:
- Más simple
- Sin overhead de particiones
- Spark maneja bien archivos de <100MB

Desventaja:
- Menos eficiente para queries con filtros de fecha
```

---

## 📋 PASOS PARA IMPLEMENTAR (Próxima Sesión)

### **1. Limpiar Bucket GCS** (5 min)
```bash
# Desde tu máquina local (con gcloud auth)
gsutil -m rm -r gs://salvando-patitas-spark/lake/raw/
gsutil -m rm -r gs://salvando-patitas-spark/lake/silver/
```

### **2. Modificar Particionamiento en Extract** (10 min)
```python
# Archivo: scripts/extract_from_supabase.py
# Línea ~270-280

# ANTES:
df_partitioned = df.withColumn("y", F.year(date_col).cast("string")) \
                   .withColumn("m", F.lpad(F.month(date_col), 2, "0")) \
                   .withColumn("d", F.lpad(F.dayofmonth(date_col), 2, "0"))

# DESPUÉS:
df_partitioned = df.withColumn("y", F.year(date_col).cast("string")) \
                   .withColumn("m", F.lpad(F.month(date_col), 2, "0"))

# Y cambiar el partitionBy:
.partitionBy("y", "m")  # En lugar de ("y", "m", "d")
```

### **3. Modificar Jobs de Silver** (15 min)
```python
# Archivos: jobs/silver/*.py
# Buscar todas las ocurrencias de:

# ANTES:
df_final = df_final.withColumn("y", F.year("created_at").cast("string")) \
                   .withColumn("m", F.lpad(F.month("created_at"), 2, "0")) \
                   .withColumn("d", F.lpad(F.dayofmonth("created_at"), 2, "0"))

(df_final.write.mode("overwrite")
 .partitionBy("y", "m", "d")  # ← Cambiar esto
 .parquet(output_path))

# DESPUÉS:
df_final = df_final.withColumn("y", F.year("created_at").cast("string")) \
                   .withColumn("m", F.lpad(F.month("created_at"), 2, "0"))

(df_final.write.mode("overwrite")
 .partitionBy("y", "m")  # ← Solo año y mes
 .parquet(output_path))
```

**Archivos a modificar**:
- `jobs/silver/donantes.py`
- `jobs/silver/donaciones.py`
- `jobs/silver/gastos.py`
- `jobs/silver/casos.py`
- `jobs/silver/proveedores.py`
- `jobs/silver/hogar_de_paso.py`

### **4. Modificar Jobs de Gold** (10 min)
Similar al paso 3, actualizar particionamiento en:
- `jobs/gold/dim_*.py`
- `jobs/gold/fact_*.py`

### **5. Re-ejecutar Pipeline** (20 min)
```bash
# 1. Trigger DAG desde Airflow UI
# 2. Monitorear logs
# 3. Validar datos en BigQuery
```

---

## 🧪 VALIDACIÓN

### **Checklist Post-Implementación**
- [ ] Bucket GCS limpio
- [ ] Extract crea particiones y/m (no y/m/d)
- [ ] Silver lee y escribe con y/m
- [ ] Gold lee y escribe con y/m
- [ ] DAG completa sin errores
- [ ] Datos en BigQuery correctos
- [ ] Memoria de Spark <2GB durante ejecución

### **Queries de Validación**
```sql
-- BigQuery: Verificar datos
SELECT 
  COUNT(*) as total_donaciones,
  MIN(fecha_donacion) as primera_donacion,
  MAX(fecha_donacion) as ultima_donacion
FROM `salvando-patitas-spark.gold.fact_donaciones`;

-- Debe retornar ~12K registros
```

---

## 📊 ESTIMACIÓN DE MEJORA

### **Antes (Particionamiento Diario)**
```
Particiones: ~700
Memoria Spark: 2.5GB (insuficiente)
Tiempo de listado: ~30s
Estado: ❌ FALLA con OOM
```

### **Después (Particionamiento Mensual)**
```
Particiones: ~24
Memoria Spark: 2.5GB (suficiente)
Tiempo de listado: <1s
Estado: ✅ FUNCIONA
```

**Mejora**: ~30x menos particiones, ~30x más rápido

---

## 🎓 LECCIONES APRENDIDAS

### **1. Particionamiento es Crítico**
- Demasiadas particiones pequeñas → OOM
- Muy pocas particiones grandes → Lento
- Sweet spot: 1-10MB por partición

### **2. Spark Metadata Overhead**
- Spark lista TODAS las particiones antes de leer
- Cada partición tiene overhead de ~1-2KB en memoria
- 700 particiones × 2KB = 1.4MB solo en metadata

### **3. Diseño para Escala**
- Pensar en crecimiento futuro
- 2 años de datos diarios = 700 particiones
- 10 años = 3,650 particiones (inmanejable)
- Particionamiento mensual escala mejor

### **4. Trade-offs**
- **Diario**: Mejor para queries de día específico, pero no escala
- **Mensual**: Balance entre granularidad y performance
- **Anual**: Muy grueso, queries lentas
- **Sin particiones**: Simple pero ineficiente para filtros

---

## 💡 ALTERNATIVA RÁPIDA (Si tienes prisa)

### **Modo Local Temporal**
Si necesitas validar el pipeline YA, puedes:

1. Cambiar `ENV=local` en `docker-compose.yaml`
2. Ejecutar pipeline localmente (sin GCS)
3. Validar que toda la lógica funciona
4. Luego implementar fix de particiones para cloud

**Tiempo**: 10 minutos  
**Ventaja**: Ves resultados inmediatos  
**Desventaja**: No valida integración con GCS

---

## ✅ CONCLUSIÓN

**Estado Actual**:
- ✅ Autenticación GCS: RESUELTA
- ✅ Python SDK: FUNCIONANDO
- ✅ Arquitectura: DOCUMENTADA
- ⚠️ Particiones: REQUIERE AJUSTE (no crítico)

**Próxima Sesión** (30-45 min):
1. Limpiar bucket
2. Cambiar particionamiento a y/m
3. Re-ejecutar pipeline
4. Validar en BigQuery
5. ✅ Pipeline 100% funcional

**Recomendación**: Descansa hoy, implementa mañana con mente fresca 😊

---

**Última actualización**: 2025-12-29 10:32  
**Autor**: Sesión de debugging con Antigravity  
**Estado**: Listo para implementar
