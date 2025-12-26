
# 💼 Resumen Ejecutivo - Plataforma de Datos Salvando Patitas

**Estado del Proyecto**: ✅ Fase 1 (Raw Layer) Completada
**Fecha Última Actualización**: 26 Diciembre 2024

---

## 🎯 Objetivo
Construir una plataforma de datos moderna y escalable en **Google Cloud Platform** que centralice la información operativa de la fundación "Salvando Patitas" (Donaciones, Casos, Gastos) para habilitar analítica avanzada y reportes confiables.

---

## ✅ Logros Alcanzados (Fase 1: Capa Raw)

Se ha implementado con éxito la **Capa de Ingesta (Bronze/Raw)**:

1.  **Data Lake Operativo**:
    *   Almacenamiento centralizado en GCS (`gs://salvando-patitas-spark-raw`).
    *   Datos históricos y actuales ingestados desde Supabase.
    *   Formato **Parquet** optimizado con particionamiento diario (`anio/mes/dia`) para eficiencia de costos y lectura.

2.  **Pipeline Incremental**:
    *   Sistema de **Watermarks** que descarga solo lo nuevo/modificado.
    *   Proceso resiliente a fallos y re-ejecutable (idempotente).

3.  **Calidad de Datos Base**:
    *   Estandarización de **Tipos de Datos**: IDs como Enteros (Int64), Fechas en UTC con precisión de microsegundos.
    *   Eliminación de inconsistencias Float/Int en identificadores.

4.  **Acceso SQL Inmediato**:
    *   Integración con **BigQuery** mediante Tablas Externas.
    *   Los analistas pueden consultar los datos crudos inmediatamente después de la carga (`SELECT * FROM raw.raw_donaciones`).

---

## 🚀 Próximos Pasos (Fase 2: Capa Silver)

Las siguientes semanas se enfocarán en la capa de transformación:

*   **Limpieza Profunda**: Reglas de negocio, estandarización de textos.
*   **Deduplicación**: Consolidación de registros (Snapshotting).
*   **Modelado**: Creación de tablas dimensionales y de hechos limpias.

---

## 📊 Métricas de Infraestructura

*   **Tablas Base**: 6 (Donaciones, Gastos, Casos, Donantes, Proveedores, Hogar de Paso).
*   **Frecuencia de Actualización**: Diaria (On-demand).
*   **Costo Estimado**: Mínimo (Uso de capa gratuita/bajo costo de GCS y BigQuery Storage).

---

**Conclusión**: La fundación cuenta ahora con una "Memoria Digital" segura y consultable, eliminando la dependencia de hojas de cálculo o consultas directas a la base de datos operativa.
