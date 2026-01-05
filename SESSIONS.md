# 📝 Diario de Sesiones de Desarrollo

Registro cronológico del desarrollo de la plataforma SPDP.

## Historial de Sesiones

### Sesión Final: Enero 04, 2026 (Documentación & Producción)
*   **Objetivo:** Finalizar documentación, verificar datos en BigQuery y preparar material para portafolio.
*   **Logros:**
    *   Verificación exitosa de datos en tabla `dashboard_financiero`.
    *   Creación de `PORTFOLIO.md` en español.
    *   Consolidación de documentación técnica (`README.md`, `RUNBOOK.md`).
    *   Optimización de reportes de costos.

### Sesión: Diciembre 28, 2025 (Fix Permisos GCS)
*   **Problema:** Error de permisos 403 escribiendo a GCS desde Docker.
*   **Solución:** Configuración correcta de Workload Identity y montaje de credenciales.

### Sesión: Diciembre 20, 2025 (Capa Silver Refinada)
*   **Objetivo:** Ingesta de datos históricos y lógica idempotente.
*   **Logros:**
    *   Implementación de lógica de watermarking robusta.
    *   Corrección de `silver_donaciones` para manejar duplicados.

### Sesión: Diciembre 16, 2025 (Fix Particionamiento)
*   **Problema:** "Small File Problem" en GCS por particionamiento diario excesivo.
*   **Solución:** Cambio a particionamiento mensual (`year=YYYY/month=MM`) reduciendo la latencia de 30m a 3m.

---
*Este documento sirve como bitácora de decisiones arquitectónicas y resolución de problemas complejos.*
