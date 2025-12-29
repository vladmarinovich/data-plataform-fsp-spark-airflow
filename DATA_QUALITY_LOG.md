# Bitácora de Calidad de Datos (Data Quality Log)

Este documento registra las discrepancias de datos identificadas y justificadas durante el desarrollo del Data Platform.

## [2025-12-28] - Deduplicación en Tabla de Gastos (Q1 2023)

### Contexto
Al validar la capa Gold, se observó que `fact_gastos` contenía 604 registros, mientras que la fuente Raw reportaba 621.

### Hallazgo de Auditoría
Se ejecutó un script de diagnóstico (`scripts/debug_gastos_leak.py`) que confirmó lo siguiente:
- **Total Raw:** 621
- **IDs Duplicados detected:** 17
- **Lógica aplicada:** `Window.partitionBy("id_gasto").orderBy(F.col("last_modified_at").desc())`

### Justificación
La diferencia de 17 registros corresponde a **duplicados técnicos** en la fuente de datos. La capa Silver eliminó estos duplicados para evitar la sobreestimación de costos en los tableros financieros, manteniendo únicamente la última versión modificada de cada gasto.

**Estado:** ✅ Validado y Justificado.
