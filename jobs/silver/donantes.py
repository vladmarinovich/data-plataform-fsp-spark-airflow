
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

def run_silver_donantes():
    spark = get_spark_session("SilverDonantes")
    try:
        print("🚀 JOB SILVER: DONANTES (MASTER)")
        input_path = f"{config.RAW_PATH}/raw_donantes"
        output_path = f"{config.SILVER_PATH}/donantes"
        
        # Leemos todo raw (Particionado Hive se lee transparente)
        # Nota: Raw donantes está particionado por fecha actualización.
        # Aquí consolidamos la ÚLTIMA versión de cada donante.
        df_raw = spark.read.parquet(input_path)
        if df_raw.rdd.isEmpty(): return

        # Transformaciones de Negocio (silver_donantes.sqlx)
        # --------------------------------------------------
        
        # Casting y Defaults (Adaptación Spark Robusta)
        def cast_to_timestamp(col_name):
            col = F.col(col_name)
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))

        df_clean = df_raw.withColumn("id_donante", F.col("id_donante").cast("long")) \
                         .withColumn("id_organizacion", F.col("id_organizacion").cast("long"))
        
        # Procesar Fechas
        df_clean = df_clean.withColumn("created_at", cast_to_timestamp("created_at")) \
                           .withColumn("last_modified_at", cast_to_timestamp("last_modified_at"))

        # Normalización y Defaults (Columnas Reales)
        df_clean = df_clean.withColumn("nombre", F.coalesce(F.upper(F.trim(F.col("nombre"))), F.lit("ANONIMO"))) \
                           .withColumn("correo", F.coalesce(F.lower(F.trim(F.col("correo"))), F.lit("desconocido"))) \
                           .withColumn("telefono", F.coalesce(F.trim(F.col("telefono")), F.lit("nn"))) \
                           .withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota"))) \
                           .withColumn("pais", F.coalesce(F.upper(F.trim(F.col("pais"))), F.lit("COLOMBIA")))

        
        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_donantes"))

        # Deduplicación Maestra
        window_spec = Window.partitionBy("id_donante").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Particionamiento derivado
        df_final = df_dedup.withColumn("anio", F.year("created_at").cast("string")) \
                           .withColumn("mes", F.lpad(F.month("created_at"), 2, "0")) \
                           .withColumn("dia", F.lpad(F.dayofmonth("created_at"), 2, "0"))

        # Escritura (Particionado por Fecha de Creación)
        (df_final.write.mode("overwrite")
         .partitionBy("anio", "mes", "dia")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
        
        # Renombrar archivos al estándar
        rename_spark_output("silver", "donantes", output_path)
        
        print("✅ Donantes procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_donantes()
