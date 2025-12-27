
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

def run_silver_donations():
    """Función principal del pipeline Silver de Donaciones."""
    
    # 1. Iniciar Spark Session con soporte GCS
    spark = get_spark_session("SilverDonaciones")
    
    try:
        print("="*80)
        print("🚀 INICIANDO JOB SILVER: DONACIONES")
        print("="*80)

        # Path de entrada (Raw) y salida (Silver)
        input_path = f"{config.RAW_PATH}/raw_donaciones.parquet"
        output_path = f"{config.SILVER_PATH}/donaciones"
        
        print(f"📥 Leyendo desde: {input_path}")
        
        # 2. Leer datos Raw (Schema evolution habilitado)
        # Spark infiere particiones anio/mes/dia automáticamente
        df_raw = spark.read.parquet(input_path)
        
        count_raw = df_raw.count()
        print(f"   📊 Registros Raw leídos: {count_raw}")
        
        if count_raw == 0:
            print("⚠️  No hay datos para procesar.")
            return

        # 3. Transformaciones y Limpieza (IMPLEMENTANDO LÓGICA DE NEGOCIO DEFINIDA)
        # -------------------------------------------------------------------------
        
        print("🛠️  Aplicando transformaciones de negocio...")
        
        # A. Normalización y Defaults (Adaptación Spark vs SQLX)
        # -------------------------------------------------------------------------
        # 1. Defaults FKs (ID 541):
        # Beneficio: Evita errores de JOIN en capas posteriores al asegurar integridad referencial.
        # Trade-off: Introduce una entidad "ficticia" en el modelo, pero previene pérdida de datos.
        
        # 2. Manejo Universal de Fechas (Adaptación Spark):
        # El raw puede venir como Long (micros de Supabase) o String (ISO local/mock).
        def cast_to_timestamp(col_name):
            col = F.col(col_name)
            # Detectar si la columna es numérica (microsegundos) usando regex
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))
            
        # Aplicamos Fallbacks iniciales
        df_clean = df_raw.withColumn("id_donacion", F.col("id_donacion").cast("long")) \
                         .withColumn("id_donante", F.when(F.col("id_donante").isNull(), 541).otherwise(F.col("id_donante").cast("long"))) \
                         .withColumn("id_caso", F.when(F.col("id_caso").isNull(), 541).otherwise(F.col("id_caso").cast("long"))) \
                         .withColumn("monto", F.col("monto").cast("double"))
                         
        # Procesar Fechas con Fallbacks
        # Fallback de created_at/last_modified_at es fecha_donacion
        df_clean = df_clean.withColumn("fecha_donacion_ts", cast_to_timestamp("fecha_donacion")) \
                           .withColumn("created_at_ts", cast_to_timestamp("created_at")) \
                           .withColumn("last_modified_at_ts", cast_to_timestamp("last_modified_at"))
        
        df_clean = df_clean.withColumn("fecha_donacion", F.col("fecha_donacion_ts")) \
                           .withColumn("created_at", F.coalesce(F.col("created_at_ts"), F.col("fecha_donacion_ts"))) \
                           .withColumn("last_modified_at", F.coalesce(F.col("last_modified_at_ts"), F.col("fecha_donacion_ts"))) \
                           .drop("fecha_donacion_ts", "created_at_ts", "last_modified_at_ts")

        # Fallback para fechas NULL aplicado arriba ↑

        # 2. Normalización de Strings (medio_pago, estado)
        # medio_pago: null -> nequi, 'transfer' -> transferencia, 'tarjeta' -> tarjeta
        df_clean = df_clean.withColumn("medio_pago_clean", F.lower(F.trim(F.col("medio_pago"))))
        
        df_clean = df_clean.withColumn("medio_pago", 
            F.when(F.col("medio_pago").isNull(), "nequi")
             .when(F.col("medio_pago_clean").like("%transfer%"), "transferencia")
             .when(F.col("medio_pago_clean").like("%tarjeta%"), "tarjeta")
             .otherwise(F.col("medio_pago_clean"))
        ).drop("medio_pago_clean")

        # estado: null -> exitoso
        df_clean = df_clean.withColumn("estado_clean", F.lower(F.trim(F.col("estado"))))
        df_clean = df_clean.withColumn("estado", 
            F.when(F.col("estado").isNull(), "exitoso")
             .otherwise(F.col("estado_clean"))
        ).drop("estado_clean")

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
        df_final = df_final.withColumn("anio", F.year("fecha_donacion")) \
                           .withColumn("mes", F.lpad(F.month("fecha_donacion"), 2, "0")) \
                           .withColumn("dia", F.lpad(F.dayofmonth("fecha_donacion"), 2, "0"))

        # 4. Escritura en Capa Silver y Cuarentena
        # -------------------------------------------------------------------------
        # A. Escribir Cuarentena (si hay datos sucios)
        if count_dirty > 0:
            quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/donaciones"
            print(f"☣️  Guardando datos sucios en: {quarantine_path}")
            df_quarantine.write.mode("append").parquet(quarantine_path)
            # Convertimos el array de errores a string para que el log sea legible
            df_quarantine.select("id_donacion", "dq_errors").show(truncate=False)

        print(f"💾 Escribiendo a Silver: {output_path}")
        
        (
            df_final.write
            .mode("overwrite")
            .partitionBy("anio", "mes", "dia")
            .format("parquet")
            .option("compression", "snappy")
            .option("partitionOverwriteMode", "dynamic")
            .save(output_path)
        )
        
        # Renombrar archivos al estándar del proyecto
        rename_spark_output("silver", "donaciones", output_path)
        
        print("✅ Escritura completada exitosamente.")
        print("="*80)

    except Exception as e:
        print(f"❌ Error en job Silver Donaciones: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_donations()
