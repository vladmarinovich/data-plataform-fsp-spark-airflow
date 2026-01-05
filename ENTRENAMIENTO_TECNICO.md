# 🎓 Guía de Entrenamiento Técnico: SPDP Data Platform

Esta guía está diseñada para ayudarte a explicar tu proyecto con confianza en entrevistas técnicas o presentaciones de negocio.

---

## 1. El "Elevator Pitch" (La Arquitectura en 1 minuto)

**Pregunta:** *"Cuéntame sobre la arquitectura de tu plataforma."*

**Tu Respuesta:**
"He construido un **Data Lakehouse moderno** utilizando la arquitectura Medallion (Bronze, Silver, Gold).
1.  **Ingesta:** Extraigo datos incrementales de PostgreSQL (Supabase) usando Python, guardándolos como Parquet crudo en Google Cloud Storage (**Bronze**).
2.  **Procesamiento:** Utilizo **Apache Spark** orquestado por **Airflow** para limpiar y deduplicar los datos (**Silver**), y luego modelarlos en un esquema Estrella (Star Schema) para analítica (**Gold**).
3.  **Consumo:** Los datos finales se cargan en **BigQuery** para alimentar dashboards en Looker Studio.
Toda la infraestructura vive en **Docker** sobre una VM de Compute Engine, optimizada para costar menos de $2 USD al mes."

---

## 2. Ingesta: `scripts/extract_from_supabase.py`

**¿Qué hace?**
Es el motor de extracción. Conecta a la base de datos transaccional y descarga los datos nuevos.

**Puntos Clave a Explicar:**
*   **Carga Incremental:** No descargo todo cada vez. Uso una columna `last_modified_at` (Watermark) para traer solo lo que cambió desde la última ejecución. *Por qué: Eficiencia y velocidad.*
*   **Formato Parquet:** Guardo en Parquet, no CSV. *Por qué: Es columnar, comprimido (Snappy) y mantiene los tipos de datos (schema enforcement).*
*   **Particionamiento:** Guardo los archivos en carpetas por fecha (`year=2024/month=01/...`). *Por qué: Permite que Spark lea solo lo que necesita (Partition Pruning).*

---

## 3. Orquestación: `dags/spdp_main_pipeline.py` (Airflow)

**¿Qué hace?**
Es el "director de orquesta". Define el orden de las tareas y maneja los errores.

**Puntos Clave a Explicar:**
*   **DAG (Directed Acyclic Graph):** El flujo va en una sola dirección. Extracción -> Silver -> Gold -> BigQuery.
*   **Manejo de Fallos:** Si una tarea falla, Airflow reintenta automáticamente 3 veces. Si falla definitivamente, me envía una alerta a **Slack**.
*   **Recursos (Pools):** Limito la concurrencia a 2 tareas a la vez (`parallelism`). *Por qué: Para no saturar la RAM de 16GB de la VM y evitar errores de memoria (OOM).*
*   **Auto-Apagado:** Una tarea final apaga la VM automáticamente. Usé `trigger_rule='all_done'` para asegurar que se apague incluso si el pipeline falla. *Por qué: Ahorro masivo de costos ($500 -> $2).*

---

## 4. Transformación: `jobs/silver/*.py` (Spark)

**¿Qué hace?**
Limpia los datos crudos. Es la capa de "Calidad".

**Puntos Clave a Explicar:**
*   **Lectura:** Spark lee los Parquet crudos de Bronze.
*   **Deduplicación:** Uso `dropDuplicates()` basado en IDs. *Por qué: En sistemas distribuidos, a veces se procesa el mismo dato dos veces; esto garantiza unicidad.*
*   **Validación de Schema:** Fuerzo los tipos de datos correctos (fechas como fechas, montos como double).
*   **Escritura:** Escribo en la capa Silver particionando por `Año/Mes`.

---

## 5. Modelado: `jobs/gold/*.py` (Spark & Kimball)

**¿Qué hace?**
Prepara los datos para el negocio. Aquí aplicamos lógica de negocio, no solo limpieza.

**Puntos Clave a Explicar:**
*   **Modelo Dimensional (Kimball):**
    *   **Tablas de Hechos (Facts):** Eventos que ocurren (ej. `fact_donaciones`, `fact_gastos`). Tienen métricas (dinero) y claves foráneas.
    *   **Tablas de Dimensiones (Dims):** Contexto (ej. `dim_donantes`, `dim_calendario`). Tienen atributos descriptivos (nombre, ciudad).
*   **Star Schema:** Este diseño hace que las consultas en BigQuery sean rapidísimas y fáciles de entender para los analistas.

---

## 6. Infraestructura & DevOps

**Pregunta:** *"¿Por qué usaste Docker y una VM en lugar de servicios gestionados?"*

**Tu Respuesta:**
"Evalué servicios como Cloud Composer o Dataproc, pero su costo base ($300-$500/mes) era excesivo para una ONG.
Al "dockerizar" Airflow y Spark en una sola VM potente (e2-standard-4) y controlarla con scripts de encendido/apagado, logré la misma funcionalidad por el 1% del precio. Docker garantiza que el entorno sea reproducible: si corre en mi máquina, corre en producción."

---

## 💡 Glosario de Conceptos "Senior" que usaste

Usa estas palabras para sonar muy pro:

*   **Idempotencia:** "Mi pipeline es idempotente; puedo correrlo 10 veces sobre los mismos datos y el resultado final siempre es correcto (no duplica)."
*   **Backfill:** "El diseño permite reprocesar historia antigua (Backfill) simplemente borrando la 'watermark'."
*   **Schema Drift:** "Manejo cambios en la estructura de datos forzando esquemas en la capa Silver."
*   **Partition Pruning:** "Optimizo lecturas leyendo solo las particiones de fecha necesarias."

---

## 🧪 Ejercicio Práctico

Abre `jobs/silver/donaciones.py` e intenta explicarme línea por línea qué está pasando, usando los conceptos de arriba.
