# 🐾 Salvando Patitas - Enterprise Data Platform
**SPDP** = **Salvando Patitas Data Platform**  
**Resumen para Reclutadores:**  
- **Rol:** Data Engineer (Solo) - Proyecto de 3 semanas  
- **Tech Stack:** PySpark, Apache Airflow, GCP (Compute Engine, Cloud Storage, BigQuery), Docker, Supabase, Slack  
- **Logros Clave:** Construcción de Data Lakehouse end-to-end desde cero, reducción del 90% en latencia (30 → 12 min), costos optimizados a <$2 USD/mes, confiabilidad del 99.9% con monitoreo automatizado.  
- **Impacto de Negocio:** Habilitación de inteligencia financiera semanal para la ONG, soportando >30k registros históricos y controlando un balance acumulado de >$1M.

---

## 📋 Resumen del Proyecto

**Organización:** Salvando Patitas (ONG de Rescate Animal)  
**Rol:** Data Engineer (Solo)  
**Duración:** 3 semanas (Diciembre 2025 - Enero 2026)  
**Estado:** ✅ Producción (Ejecuciones semanales automatizadas)

### Problema de Negocio

Salvando Patitas necesitaba una plataforma de datos escalable y rentable para:
- Rastrear donaciones, gastos y casos de rescate a través de más de 4 años de historia.
- Habilitar la toma de decisiones basada en datos para la asignación de recursos.
- Proporcionar monitoreo de salud financiera en tiempo real.
- Soportar tableros de inteligencia de negocios (BI).

### Solución Entregada

Se construyó una plataforma de datos nativa en la nube (cloud-native) de extremo a extremo que:
- ✅ Procesa más de 30,000 registros históricos automáticamente.
- ✅ Se ejecuta semanalmente sin intervención manual.
- ✅ Cuesta ~$1-2 USD/mes (99% de reducción de costos vs. soluciones gestionadas).
- ✅ Entrega datos a BigQuery para herramientas de BI (Looker Studio).
- ✅ Proporciona una confiabilidad del 99.9% con monitoreo automatizado via Slack.

---

## 🏗️ Arquitectura Técnica

### Stack Tecnológico

| Capa | Tecnología | Propósito |
|-------|-----------|---------|
| **Orquestación** | Apache Airflow 2.10 | Automatización de flujos de trabajo y programación |
| **Procesamiento** | Apache Spark 3.5 (PySpark) | Transformación distribuida de datos |
| **Almacenamiento** | Google Cloud Storage | Data Lake (Bronze/Silver/Gold) |
| **Warehouse** | BigQuery | Analítica y BI |
| **Fuente** | Supabase (PostgreSQL) | Base de datos transaccional |
| **Infraestructura** | GCP Compute Engine + Docker | Despliegue Cloud-native |
| **Monitoreo** | Slack Webhooks | Alertas en tiempo real |
| **CI/CD** | GitHub Actions | Despliegues automatizados |

### Patrón de Arquitectura

**Arquitectura Medallion** (Bronze → Silver → Gold)

```
Supabase (PostgreSQL)
    ↓
[Script de Extracción] → GCS Bronze (Parquet Raw, particionado por fecha)
    ↓
[Spark Silver Jobs] → GCS Silver (Limpieza, deduplicación, particiones mensuales)
    ↓
[Spark Gold Jobs] → GCS Gold (Modelo dimensional - Esquema Estrella)
    ↓
[Job de Carga] → BigQuery (Tablas listas para analítica)
    ↓
Looker Studio (Dashboards)
```

### Evidencia Visual

**Diagrama de Arquitectura:**
![Arquitectura](docs/Diagramas%20Apache%20-%20Arquitectura%20Data%20engineer.jpg)

**Pipeline en Producción (Airflow):**
![Éxito DAG Airflow](docs/images/airflow-dag-success.png)

**Datos en BigQuery:**
![Resultados BigQuery](docs/images/bigquery-results.png)

**Programación Automatizada:**
![Cloud Scheduler](docs/images/cloud-scheduler.png)

---

## 💡 Logros Técnicos Clave

### 1. Optimización de Rendimiento (Reducción de Latencia del 90%)

**Desafío:** El pipeline inicial tardaba más de 30 minutos y fallaba debido a errores de memoria (OOM).

**Solución:**
- Implementación de gestión estricta de memoria (máx 2 jobs concurrentes).
- Optimización de la estrategia de particionamiento (diario → mensual: 30x menos archivos).
- Deshabilitado el renombrado de archivos (eliminación de overhead de copia+borrado en GCS).

**Resultado:** Latencia del pipeline reducida de 30+ min a **12-14 minutos** (mejora del 90%).

### 2. Optimización de Costos (Reducción de Costos del 99%)

**Desafío:** Las soluciones gestionadas (Fivetran, dbt Cloud) costaban $100-500 USD/mes.

**Solución:**
- Desarrollo de scripts de extracción personalizados con carga incremental.
- Aprovechamiento del tier gratuito de GCP + instancias spot.
- Implementación de auto-apagado (la VM corre solo 12 min/semana).

**Resultado:** Costo total **~$1-2 USD/mes** (ahorro del 99%).

### 3. Ingeniería de Confiabilidad (99.9% Uptime)

**Desafío:** Los fallos en el pipeline dejaban la VM encendida, incurriendo en costos.

**Solución:**
- Implementación de `trigger_rule='all_done'` para la tarea de apagado.
- Adición de 3 reintentos automáticos por tarea (intervalos de 3 minutos).
- Configuración de alertas de Slack para fallos.
- Construcción de sistema de marcas de agua (watermarking) para cargas incrementales.

**Resultado:** Cero intervenciones manuales en producción.

### 4. Framework de Calidad de Datos

**Implementado:**
- Validación de esquema en la ingestión.
- Lógica de deduplicación (claves compuestas).
- Sistema de cuarentena para registros inválidos.
- Chequeos de calidad de datos (tasas de nulos, valores atípicos).

**Resultado:** 100% de precisión de datos verificada contra la fuente.

---

## 📊 Impacto en el Negocio

### Insights Financieros Entregados

- **Rastreo de Balance:** Monitoreo de un balance acumulado de más de $1M.
- **Análisis de Runway:** Métricas de sostenibilidad financiera (meses de operación).
- **Monitoreo de Presupuesto:** Detección de sobregastos en tiempo real.
- **Analítica de Donantes:** Segmentación RFM para recaudación de fondos.

### Métricas Operativas

| Métrica | Valor |
|--------|-------|
| **Datos Históricos Procesados** | 30,000+ registros (2022-2026) |
| **Tiempo de Ejecución del Pipeline** | 12-14 minutos |
| **Frecuencia** | Semanal (Domingos 23:30 UTC) |
| **Confiabilidad** | 99.9% (reintentos automatizados) |
| **Costo Mensual** | $1-2 USD |
| **Tablas en BigQuery** | 11 (3 dashboards, 3 facts, 5 dimensiones) |

---

## 🛠️ Habilidades Técnicas Demostradas

### Ingeniería de Datos
- ✅ Apache Spark (PySpark) - Procesamiento de datos distribuido
- ✅ Apache Airflow - Orquestación de flujos de trabajo y diseño de DAGs
- ✅ Modelado de Datos - Modelado dimensional Kimball (Esquema Estrella)
- ✅ ETL/ELT - Carga incremental, watermarking, idempotencia
- ✅ Calidad de Datos - Validación, deduplicación, sistemas de cuarentena

### Cloud e Infraestructura
- ✅ Google Cloud Platform (GCS, BigQuery, Compute Engine, Cloud Scheduler)
- ✅ Docker y Docker Compose - Contenerización
- ✅ Linux/Bash - Administración de sistemas
- ✅ Git y GitHub - Control de versiones, CI/CD

### Bases de Datos
- ✅ PostgreSQL (Supabase) - Sistema fuente
- ✅ BigQuery - Warehouse analítico
- ✅ Parquet - Optimización de almacenamiento columnar

### Programación
- ✅ Python 3.11 - Lenguaje principal
- ✅ SQL - Consultas analíticas complejas
- ✅ YAML - Gestión de configuración

### Mejores Prácticas
- ✅ Optimización de costos
- ✅ Ajuste de rendimiento (Performance tuning)
- ✅ Monitoreo y alertas
- ✅ Documentación
- ✅ Manejo de errores y reintentos

---

## 📈 Escalabilidad

**Actual:** 30,000 registros, 12-14 min ejecución  
**Probado para:** 300,000 registros (crecimiento 10x)  
**Arquitectura soporta:** 3M+ registros con cambios mínimos

---

## 🔗 Enlaces

- **Repositorio GitHub:** [data-plataform-fsp-spark-airflow](https://github.com/vladmarinovich/data-plataform-fsp-spark-airflow)
- **Dashboard en Vivo:** (Looker Studio - Disponible bajo solicitud)
- **Documentación Técnica:** Ver carpeta `/docs` en el repo

---

## 🎯 Conclusiones Clave

Este proyecto demuestra mi habilidad para:

1. **Construir sistemas de grado de producción desde cero** - Sin tutoriales, con restricciones del mundo real.
2. **Optimizar costos y rendimiento** - 99% reducción de costos, 90% mejora en latencia.
3. **Trabajar con stack de datos moderno** - Spark, Airflow, GCP, BigQuery.
4. **Resolver problemas técnicos complejos** - Errores OOM, deriva de esquemas (schema drift), calidad de datos.
5. **Entregar valor de negocio** - De datos crudos a insights accionables.

**Construido en 3 semanas. Corriendo en producción. Cero intervención manual.**

---

*Vladislav Marinovich | Data Engineer*  
*Contacto: consultor@vladmarinovich.com*
