
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
from jobs.utils.file_utils import rename_spark_output

def run_silver_casos():
    spark = get_spark_session("SilverCasos")
    try:
        print("🚀 JOB SILVER: CASOS")
        input_path = f"{config.RAW_PATH}/raw_casos"
        output_path = f"{config.SILVER_PATH}/casos"
        
        df_raw = spark.read.parquet(input_path)
        if df_raw.rdd.isEmpty(): return

        # Transformaciones de Negocio (silver_casos.sqlx)
        # -----------------------------------------------
        
        # Casting y Defaults Numéricos (Adaptación Spark Robusta)
        def cast_to_timestamp(col_name):
            col = F.col(col_name)
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))

        df_clean = df_raw.withColumn("id_caso", F.col("id_caso").cast("long")) \
                         .withColumn("id_hogar_de_paso", F.coalesce(F.col("id_hogar_de_paso").cast("long"), F.lit(9))) \
                         .withColumn("presupuesto_estimado", F.coalesce(F.col("presupuesto_estimado").cast("double"), F.lit(3500000.0)))
        
        # Procesar Fechas
        df_clean = df_clean.withColumn("fecha_ingreso", cast_to_timestamp("fecha_ingreso")) \
                           .withColumn("fecha_salida", cast_to_timestamp("fecha_salida")) \
                           .withColumn("last_modified_at", cast_to_timestamp("last_modified_at")) \
                           .withColumn("created_at", cast_to_timestamp("created_at"))

        # Normalización Strings y Defaults
        # nombre_caso
        df_clean = df_clean.withColumn("nombre_caso", F.coalesce(F.trim(F.lower(F.col("nombre_caso"))), F.lit("anonimo")))
        
        # veterinaria
        df_clean = df_clean.withColumn("veterinaria", F.coalesce(F.lower(F.trim(F.col("veterinaria"))), F.lit("cba")))
        
        # diagnostico
        df_clean = df_clean.withColumn("diagnostico", F.coalesce(F.lower(F.trim(F.col("diagnostico"))), F.lit("reservado")))
        
        # ciudad
        df_clean = df_clean.withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota")))
        
        # estado (Fijo según SQLX)
        df_clean = df_clean.withColumn("estado", F.lit("no urgente"))

        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_casos"))

        # Deduplicación
        window_spec = Window.partitionBy("id_caso").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Particionamiento derivado (Fecha Ingreso)
        df_final = df_dedup.withColumn("anio", F.year("fecha_ingreso")) \
                           .withColumn("mes", F.lpad(F.month("fecha_ingreso"), 2, "0")) \
                           .withColumn("dia", F.lpad(F.dayofmonth("fecha_ingreso"), 2, "0"))

        (df_final.write.mode("overwrite").partitionBy("anio", "mes", "dia")
         .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        
        # Renombrar archivos al estándar
        rename_spark_output("silver", "casos", output_path)
        
        print("✅ Casos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_casos()
