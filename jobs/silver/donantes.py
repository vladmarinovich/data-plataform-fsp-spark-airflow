
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

        # Transformaciones
        df_clean = df_raw.withColumn("id_donante", F.col("id_donante").cast("long")) \
                         .withColumn("consentimiento", F.col("consentimiento").cast("boolean")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp")) \
                         .withColumn("created_at", F.col("created_at").cast("timestamp"))

        # Limpieza (Drop sensitive/heavy cols if needed, but we keep them for Silver)
        # Si quisieramos anonimizar, sería aquí.

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
