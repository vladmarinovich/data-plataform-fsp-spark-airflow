
"""
Job Silver: Transformación y Limpieza de Gastos.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session

def run_silver_gastos():
    spark = get_spark_session("SilverGastos")
    try:
        print("🚀 JOB SILVER: GASTOS")
        input_path = f"{config.RAW_PATH}/raw_gastos"
        output_path = f"{config.SILVER_PATH}/gastos"
        
        df_raw = spark.read.option("basePath", input_path).parquet(input_path + "/*")
        if df_raw.rdd.isEmpty(): return

        # Transformaciones
        df_clean = df_raw.withColumn("id_gasto", F.col("id_gasto").cast("long")) \
                         .withColumn("id_caso", F.col("id_caso").cast("long")) \
                         .withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("monto", F.col("monto").cast("double")) \
                         .withColumn("fecha_pago", F.col("fecha_pago").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))

        # Limpieza Strings
        for col in ["nombre_gasto", "medio_pago", "estado", "comprobante"]:
            df_clean = df_clean.withColumn(col, F.trim(F.col(col)))
            df_clean = df_clean.withColumn(col, F.when(F.col(col) == "nan", None).otherwise(F.col(col)))

        # Deduplicación
        window_spec = Window.partitionBy("id_gasto").orderBy(F.col("last_modified_at").desc())
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

        # Particionamiento derivado
        df_final = df_dedup.withColumn("anio_part", F.year("fecha_pago")) \
                           .withColumn("mes_part", F.month("fecha_pago"))

        # Escritura
        (df_final.write.mode("overwrite").partitionBy("anio_part", "mes_part")
         .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        print("✅ Gastos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_gastos()
