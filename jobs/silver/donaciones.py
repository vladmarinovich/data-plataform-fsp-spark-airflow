
"""
Job Silver: Transformación y Limpieza de Donaciones.

Objetivo:
- Leer datos crudos desde RAW.
- Aplicar reglas de limpieza y calidad de datos.
- Deduplicar registros (CDC).
- Escribir tabla limpia en capa SILVER.

Ejecución:
$ spark-submit --master local[*] jobs/silver/donaciones.py
"""
import sys
from pathlib import Path

# Agregar raíz del proyecto al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output
from jobs.utils.watermark import get_watermark, update_watermark

def run_silver_donations():
    """Función principal del pipeline Silver de Donaciones."""
    
    # 1. Iniciar Spark Session con soporte GCS
    spark = get_spark_session("SilverDonaciones")
    
    try:
        print("="*80)
        print("🚀 INICIANDO JOB SILVER: DONACIONES")
        print("="*80)

        # 2. Definir paths de entrada (Raw) y salida (Silver)
        input_path = f"{config.RAW_PATH}/donaciones"
        output_path = f"{config.SILVER_PATH}/donaciones"
        
        # 3. Leer datos Raw con INFERENCIA DE SCHEMA (mergeSchema)
        # ESTRATEGIA FLEXIBLE: No imponemos schema. Dejamos que Spark detecte tipos.
        # Si hay mezcla de INT64 y DOUBLE (por nulos de Pandas), Spark resolverá a DOUBLE automáticamente.
        # Esto evita el error del lector vectorizado estricto.
        
        df_raw = spark.read.option("mergeSchema", "true").parquet(input_path)

        # Corregir tipos de datos: Lo que sea que Spark haya inferido -> Double -> Long
        # Esto maneja tanto si infirió Double (mezcla) como si infirió Long (limpio).
        df_raw = df_raw.withColumn("id_donante", F.col("id_donante").cast(T.DoubleType()).cast(T.LongType())) \
                       .withColumn("id_caso", F.col("id_caso").cast(T.DoubleType()).cast(T.LongType())) \
                       .withColumn("fecha_donacion", F.to_timestamp(F.col("fecha_donacion"))) \
                       .withColumn("created_at", F.to_timestamp(F.col("created_at"))) \
                       .withColumn("last_modified_at", F.to_timestamp(F.col("last_modified_at")))
        
        # 4. Filtrar por watermark (después de convertir fechas)
        watermark = get_watermark(spark, "donaciones")
        if watermark:
            df_raw = df_raw.filter(F.col("last_modified_at") > watermark)
        
        # 5. (Opcional) Filtrar solo el mes de prueba si TEST_MONTH está definido
        if config.TEST_MONTH:
            year, month = config.TEST_MONTH.split("-")
            df_raw = df_raw.filter(
                (F.year("fecha_donacion") == int(year)) & 
                (F.month("fecha_donacion") == int(month))
            )
        
        print(f"📥 Lectura completada, filas: {df_raw.count()}")
        
        count_raw = df_raw.count()
        print(f"   📊 Registros Raw leídos: {count_raw}")
        
        if count_raw == 0:
            print("⚠️  No hay datos para procesar.")
            return


        # 3. Transformaciones y Limpieza (IMPLEMENTANDO LÓGICA DE NEGOCIO DEFINIDA)
        # -------------------------------------------------------------------------
        
        print("🛠️  Aplicando transformaciones de negocio...")
        
        # A. Normalización y Defaults
        # -------------------------------------------------------------------------
        # 1. Defaults FKs (ID 541 - "Sin Dato" / "General") - Evita errores de JOIN
        # 2. Fallbacks de fechas: created_at y last_modified_at usan fecha_donacion si son NULL
        
        # Nota: Los IDs ya vienen como LongType del bloque de lectura (L64), solo aplicamos Coalesce.
        df_clean = df_raw.withColumn("id_donacion", F.col("id_donacion")) \
                         .withColumn("id_donante", F.coalesce(F.col("id_donante"), F.lit(541))) \
                         .withColumn("id_caso", F.coalesce(F.col("id_caso"), F.lit(541))) \
                         .withColumn("monto", F.col("monto").cast("double")) \
                         .withColumn("created_at", F.coalesce(F.col("created_at"), F.col("fecha_donacion"))) \
                         .withColumn("last_modified_at", F.coalesce(F.col("last_modified_at"), F.col("fecha_donacion")))


        # 2. Normalización de Strings (medio_pago, estado)
        # medio_pago: null -> nequi, 'transfer' -> transferencia, 'tarjeta' -> tarjeta
        df_clean = df_clean.withColumn("medio_pago_clean", F.lower(F.trim(F.col("medio_pago"))))
        
        df_clean = df_clean.withColumn("medio_pago", 
            F.when(F.col("medio_pago").isNull(), "nequi")
             .when(F.col("medio_pago_clean").like("%transfer%"), "transferencia")
             .when(F.col("medio_pago_clean").like("%tarjeta%"), "tarjeta")
             .otherwise(F.col("medio_pago_clean"))
        ).drop("medio_pago_clean")

        # estado: Normalización robusta (Unificar estados de CRM y Pasarelas)
        success_states = ['aprobada', 'confirmada', 'confirmado', 'exitosa', 'exitoso', 'completada']
        fail_states = ['fallida', 'rechazada', 'cancelada', 'error']
        
        df_clean = df_clean.withColumn("estado_raw", F.lower(F.trim(F.col("estado"))))
        df_clean = df_clean.withColumn("estado", 
            F.when(F.col("estado_raw").isin(success_states) | F.col("estado").isNull(), "completada")
             .when(F.col("estado_raw").isin(fail_states), "fallida")
             .otherwise("pendiente")
        ).drop("estado_raw")

        # 3. Columnas de Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_donaciones"))

        # C. Lógica de deduplicación (Snapshotting CDC)
        print("🔄 Deduplicando por ID y fecha de modificación...")
        
        window_spec = Window.partitionBy("id_donacion").orderBy(F.col("last_modified_at").desc())
        
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)) \
                           .filter(F.col("row_num") == 1) \
                           .drop("row_num")
        
        count_dedup = df_dedup.count()
        print(f"   ✅ Registros tras deduplicación: {count_dedup} (Descartados: {count_raw - count_dedup})")

        # D. Calidad de Datos (Data Quality - DQ) y Cuarentena
        # -------------------------------------------------------------------------
        print("🛡️  Validando calidad de datos (Assertions)...")
        
        # Definimos las reglas de validación
        df_dq = df_dedup.withColumn("dq_errors", F.array())
        
        # Regla 1: Monto no negativo
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("monto") < 0, 
                F.array_union(F.col("dq_errors"), F.array(F.lit("MONTO_NEGATIVO")))
            ).otherwise(F.col("dq_errors"))
        )
        
        # Regla 2: Fecha de donación no futura
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("fecha_donacion") > F.current_timestamp(),
                F.array_union(F.col("dq_errors"), F.array(F.lit("FECHA_FUTURA")))
            ).otherwise(F.col("dq_errors"))
        )
        
        # Regla 3: ID de donación debe existir
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("id_donacion").isNull(),
                F.array_union(F.col("dq_errors"), F.array(F.lit("ID_NULO")))
            ).otherwise(F.col("dq_errors"))
        )

        # Regla 4: Fecha mínima (2010) - Proveniente de Dataform SQLX
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("fecha_donacion") < F.lit("2010-01-01"),
                F.array_union(F.col("dq_errors"), F.array(F.lit("FECHA_ANTERIOR_A_2010")))
            ).otherwise(F.col("dq_errors"))
        )

        # Dividimos el dataset: Silver vs Cuarentena
        df_final = df_dq.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        df_quarantine = df_dq.filter(F.size(F.col("dq_errors")) > 0)
        
        count_silver = df_final.count()
        count_dirty = df_quarantine.count()
        
        print(f"   ✨ Registros Válidos (Silver): {count_silver}")
        print(f"   ☣️  Registros en Cuarentena: {count_dirty}")

        # E. Generación de Particiones para escritura
        # Primero eliminamos columnas de partición si existen (vienen del RAW)
        cols_to_drop = [c for c in ["y", "m", "d"] if c in df_final.columns]
        if cols_to_drop:
            df_final = df_final.drop(*cols_to_drop)
        
        # Ahora creamos las columnas de partición
        df_final = df_final.withColumn("y", F.year("fecha_donacion")) \
                           .withColumn("m", F.lpad(F.month("fecha_donacion"), 2, "0")) \
                           .withColumn("d", F.lpad(F.dayofmonth("fecha_donacion"), 2, "0"))       # A. Escribir Cuarentena (si hay datos sucios)
        if count_dirty > 0:
            quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/donaciones"
            print(f"☣️  Guardando datos sucios en: {quarantine_path}")
            df_quarantine.write.mode("append").parquet(quarantine_path)
            # Convertimos el array de errores a string para que el log sea legible
            df_quarantine.select("id_donacion", "dq_errors").show(truncate=False)

        print(f"💾 Escribiendo a Silver: {output_path}")
        
        (
            df_final.write
            .mode("append") # Append para acumular ficheros diarios en la carpeta mensual
            .partitionBy("y", "m")
            .format("parquet")
            .option("compression", "snappy")
            .option("partitionOverwriteMode", "dynamic")
            .save(output_path)
        )
        
        # Renombrar archivos al estándar del proyecto
        rename_spark_output("silver", "donaciones", output_path)

        # Update watermark logic disabled inside job (Orchestrator handles state)
        # max_ts = df_final.agg(F.max("last_modified_at")).collect()[0][0]
        # if max_ts:
        #     update_watermark(spark, max_ts, "donaciones")
        
        print("✅ Escritura completada exitosamente.")
        print("="*80)

    except Exception as e:
        print(f"❌ Error en job Silver Donaciones: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_donations()
