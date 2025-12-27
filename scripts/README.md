# Manual Pipeline Execution

Script para ejecutar el pipeline completo **sin Airflow**, útil para debugging.

## Uso

```bash
# Ejecutar todo el pipeline
./scripts/run_pipeline.sh

# Ver logs de un job fallido
cat /tmp/pipeline_<job_name>.log
```

## Características

- ✅ **Ejecución en orden correcto** (respeta dependencias)
- ✅ **Timeout de 60s por job** (evita cuelgues)
- ✅ **Logs guardados** en `/tmp/pipeline_*.log`
- ✅ **Output con colores** (verde=success, rojo=fail)
- ✅ **Continúa aunque falle** (no se detiene en primer error)
- ✅ **Resumen final** de qué falló

## Orden de Ejecución

1. **Silver** (6 jobs en paralelo lógico)
2. **Gold Dimensions** (5 jobs)
3. **Gold Facts** (2 jobs)
4. **Gold Features** (3 jobs)
5. **Gold Dashboards** (3 jobs)

## Debugging

Si un job falla:
```bash
# Ver error completo
cat /tmp/pipeline_<nombre_job>.log

# Ejecutar manualmente con más detalle
ENV=local python3 jobs/gold/<job>.py
```

## Ventajas vs Airflow

- 🚀 **10x más rápido** (sin overhead de Airflow)
- 🔍 **Errores más claros** (stacktraces directos)
- 🛠️ **Fácil debugear** (logs simples)
- ⚡ **Iteración rápida** (sin restart de Docker)
