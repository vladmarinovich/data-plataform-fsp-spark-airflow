
"""
Job Silver: Transformación Maestro de Donantes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output
from jobs.utils.watermark import get_watermark, update_watermark

def run_silver_donantes():
    spark = get_spark_session("SilverDonantes")
    try:
        print("🚀 JOB SILVER: DONANTES (MASTER)")
        input_path = f"{config.RAW_PATH}/donantes"
        output_path = f"{config.SILVER_PATH}/donantes"
        
        # Leemos raw - Spark usará partition pruning automáticamente
        df_raw = spark.read.parquet(input_path)
        
        # Donantes es tabla maestra: procesamos todos los registros
        # Spark solo leerá las particiones necesarias gracias a partition pruning
            
        if df_raw.rdd.isEmpty(): 
            print("⚠️ No hay donantes para procesar.")
            return

        # Transformaciones de Negocio (silver_donantes.sqlx)
        # ... (Resto del código igual) ...

        
        # Casting y Defaults (Adaptación Spark Robusta)
        def cast_to_timestamp(col_name):
            """Convierte columna a timestamp. Fechas vienen como STRING ISO desde Supabase."""
            return F.to_timestamp(F.col(col_name))

        df_clean = df_raw.withColumn("id_donante", F.col("id_donante").cast("long"))
        
        # Procesar Fechas
        df_clean = df_clean.withColumn("created_at", cast_to_timestamp("created_at")) \
                           .withColumn("last_modified_at", cast_to_timestamp("last_modified_at"))

        # Normalización y Defaults (Columnas Reales)
        # Mapear 'donante' (raw) a 'donante' (silver)
        df_clean = df_clean.withColumn("donante", F.coalesce(F.upper(F.trim(F.col("donante"))), F.lit("ANONIMO"))) \
                           .withColumn("correo", F.coalesce(F.lower(F.trim(F.col("correo"))), F.lit("desconocido"))) \
                           .withColumn("telefono", F.coalesce(F.trim(F.col("telefono")), F.lit("nn"))) \
                           .withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota"))) \
                           .withColumn("pais", F.when(F.col("pais").isNull() | (F.trim(F.col("pais")) == ""), "Colombia")
                                               .otherwise(F.initcap(F.trim(F.col("pais"))))) \
                           .withColumn("canal_origen", F.coalesce(F.lower(F.trim(F.col("canal_origen"))), F.lit("desconocido")))

        
        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_donantes"))

        # Deduplicación Maestra
        window_spec = Window.partitionBy("id_donante").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # D. Calidad de Datos (DQ) y Cuarentena
        # --------------------------------------------------
        print("🛡️  Validando calidad de donantes...")
        
        df_dq = df_dedup.withColumn("dq_errors", F.array())
        
        # Regla 1: Email debe tener formato mínimo (contener @)
        df_dq = df_dq.withColumn("dq_errors", 
            F.when((F.col("correo") != "desconocido") & (~F.col("correo").like("%@%")), 
                F.array_union(F.col("dq_errors"), F.array(F.lit("EMAIL_INVALIDO")))
            ).otherwise(F.col("dq_errors"))
        )
        
        # Regla 2: ID de donante no nulo
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("id_donante").isNull(),
                F.array_union(F.col("dq_errors"), F.array(F.lit("ID_NULO")))
            ).otherwise(F.col("dq_errors"))
        )

        # Dividimos: Silver vs Cuarentena
        df_final = df_dq.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        df_quarantine = df_dq.filter(F.size(F.col("dq_errors")) > 0)
        
        # Cacheamos el DF final porque lo usamos para contar y luego para escribir
        df_final.cache()
        
        count_silver = df_final.count()
        count_dirty = df_quarantine.count()
        
        print(f"   ✨ Donantes Válidos: {count_silver}")
        print(f"   ☣️  Donantes en Cuarentena: {count_dirty}")

        # E. Escritura y Particionamiento
        if count_dirty > 0:
            quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/donantes"
            print(f"☣️  Guardando donantes sospechosos en: {quarantine_path}")
            df_quarantine.write.mode("append").parquet(quarantine_path)
            df_quarantine.select("id_donante", "donante", "dq_errors").show(truncate=False)

        # Derivar particiones para el DF Final
        # Primero eliminamos columnas de partición si existen (vienen del RAW)
        cols_to_drop = [c for c in ["y", "m", "d"] if c in df_final.columns]
        if cols_to_drop:
            df_final = df_final.drop(*cols_to_drop)
        
        # Ahora creamos las columnas de partición
        # Agregamos particionamiento mensual (y, m) y columna de día para pruning (consistencia con Facts)
        df_final = df_final.withColumn("y", F.year("created_at")) \
                           .withColumn("m", F.lpad(F.month("created_at"), 2, "0")) \
                           .withColumn("d", F.lpad(F.dayofmonth("created_at"), 2, "0"))

        # Escritura Silver
        (df_final.write.mode("overwrite")
         .partitionBy("y", "m")
         .parquet(output_path))
        
        # Renombrar archivos al estándar (DESACTIVADO para rendimiento GCS)
        # rename_spark_output("silver", "donantes", output_path)

        # Update watermark (Disabled - Orchestrator managed)
        # max_ts = df_final.agg(F.max("last_modified_at")).collect()[0][0]
        # if max_ts:
        #     update_watermark(spark, max_ts, "donantes")
        
        print("✅ Donantes procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_donantes()
