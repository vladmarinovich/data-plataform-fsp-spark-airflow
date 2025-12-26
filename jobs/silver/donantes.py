
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

def run_silver_donantes():
    spark = get_spark_session("SilverDonantes")
    try:
        print("🚀 JOB SILVER: DONANTES (MASTER)")
        input_path = f"{config.RAW_PATH}/raw_donantes"
        output_path = f"{config.SILVER_PATH}/donantes"
        
        # Leemos todo raw (Particionado Hive se lee transparente)
        # Nota: Raw donantes está particionado por fecha actualización.
        # Aquí consolidamos la ÚLTIMA versión de cada donante.
        df_raw = spark.read.option("basePath", input_path).parquet(input_path + "/*")
        if df_raw.rdd.isEmpty(): return

        # Transformaciones de Negocio (silver_donantes.sqlx)
        # --------------------------------------------------
        
        # Casting
        df_clean = df_raw.withColumn("id_donante", F.col("id_donante").cast("long")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp")) \
                         .withColumn("created_at", F.col("created_at").cast("timestamp"))

        # Normalización y Defaults
        df_clean = df_clean.withColumn("donante", F.coalesce(F.upper(F.trim(F.col("donante"))), F.lit("ANONIMO"))) \
                           .withColumn("tipo_id", F.coalesce(F.lower(F.trim(F.col("tipo_id"))), F.lit("nn"))) \
                           .withColumn("identificacion", F.coalesce(F.lower(F.trim(F.col("identificacion"))), F.lit("nn"))) \
                           .withColumn("correo", F.coalesce(F.lower(F.trim(F.col("correo"))), F.lit("desconocido"))) \
                           .withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota"))) \
                           .withColumn("tipo_donante", F.coalesce(F.lower(F.trim(F.col("tipo_donante"))), F.lit("unico"))) \
                           .withColumn("pais", F.coalesce(F.upper(F.trim(F.col("pais"))), F.lit("COLOMBIA"))) \
                           .withColumn("canal_origen", F.coalesce(F.lower(F.trim(F.col("canal_origen"))), F.lit("organico"))) \
                           .withColumn("consentimiento", F.coalesce(F.col("consentimiento").cast("boolean"), F.lit(True)))

        
        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_donantes"))

        # Deduplicación Maestra
        window_spec = Window.partitionBy("id_donante").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Remover particiones viejas de hive (anio, mes, dia) si se leyeron, para no ensuciar la master
        cols_to_drop = [c for c in ["anio", "mes", "dia"] if c in df_dedup.columns]
        df_final = df_dedup.drop(*cols_to_drop)

        # Escritura (Sin partición, Overwrite Full)
        # Es una tabla dimensional < 1GB, no requiere partición.
        (df_final.write.mode("overwrite").parquet(output_path))
        print("✅ Donantes procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_donantes()
