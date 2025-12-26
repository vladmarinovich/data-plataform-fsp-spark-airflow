
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

        # Transformaciones
        df_clean = df_raw.withColumn("id_caso", F.col("id_caso").cast("long")) \
                         .withColumn("id_hogar_de_paso", F.col("id_hogar_de_paso").cast("long")) \
                         .withColumn("presupuesto_estimado", F.col("presupuesto_estimado").cast("double")) \
                         .withColumn("fecha_ingreso", F.col("fecha_ingreso").cast("timestamp")) \
                         .withColumn("fecha_salida", F.col("fecha_salida").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))

        for col in ["nombre_caso", "estado", "veterinaria", "diagnostico", "ciudad"]:
            df_clean = df_clean.withColumn(col, F.trim(F.col(col)))
            df_clean = df_clean.withColumn(col, F.when(F.col(col) == "nan", None).otherwise(F.col(col)))

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
