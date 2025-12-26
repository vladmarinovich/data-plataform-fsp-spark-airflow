
"""
Job Silver: Transformación y Limpieza de Casos.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session

def run_silver_casos():
    spark = get_spark_session("SilverCasos")
    try:
        print("🚀 JOB SILVER: CASOS")
        input_path = f"{config.RAW_PATH}/raw_casos"
        output_path = f"{config.SILVER_PATH}/casos"
        
        df_raw = spark.read.option("basePath", input_path).parquet(input_path + "/*")
        if df_raw.rdd.isEmpty(): return

        # Transformaciones de Negocio (silver_casos.sqlx)
        # -----------------------------------------------
        
        # Casting y Defaults Numéricos
        df_clean = df_raw.withColumn("id_caso", F.col("id_caso").cast("long")) \
                         .withColumn("id_hogar_de_paso", F.coalesce(F.col("id_hogar_de_paso").cast("long"), F.lit(9))) \
                         .withColumn("presupuesto_estimado", F.coalesce(F.col("presupuesto_estimado").cast("double"), F.lit(3500000.0))) \
                         .withColumn("fecha_ingreso", F.col("fecha_ingreso").cast("timestamp")) \
                         .withColumn("fecha_salida", F.col("fecha_salida").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))

        # Normalización Strings y Defaults
        # nombre_caso
        df_clean = df_clean.withColumn("nombre_caso", F.coalesce(F.trim(F.col("nombre_caso")), F.lit("desconocido")))
        
        # veterinaria
        df_clean = df_clean.withColumn("veterinaria", F.coalesce(F.lower(F.trim(F.col("veterinaria"))), F.lit("desconocido")))
        
        # diagnostico
        df_clean = df_clean.withColumn("diagnostico", F.coalesce(F.lower(F.trim(F.col("diagnostico"))), F.lit("reservado")))
        
        # ciudad
        df_clean = df_clean.withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota")))
        
        # estado
        df_clean = df_clean.withColumn("estado", F.lower(F.trim(F.col("estado"))))

        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_casos"))

        # Deduplicación
        window_spec = Window.partitionBy("id_caso").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Particionamiento derivado (Fecha Ingreso)
        df_final = df_dedup.withColumn("anio_part", F.year("fecha_ingreso")) \
                           .withColumn("mes_part", F.month("fecha_ingreso"))

        (df_final.write.mode("overwrite").partitionBy("anio_part", "mes_part")
         .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        print("✅ Casos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_casos()
