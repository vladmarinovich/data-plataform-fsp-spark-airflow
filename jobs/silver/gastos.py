
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

        # Transformaciones y Reglas de Negocio (Según silver_gastos.sqlx)
        # ---------------------------------------------------------------
        
        # 1. Casting y Defaults
        df_clean = df_raw.withColumn("id_gasto", F.col("id_gasto").cast("long")) \
                         .withColumn("id_caso", F.coalesce(F.col("id_caso").cast("long"), F.lit(541))) \
                         .withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("monto", F.col("monto").cast("double")) \
                         .withColumn("fecha_pago", F.col("fecha_pago").cast("timestamp")) \
                         .withColumn("last_modified_at", F.col("last_modified_at").cast("timestamp"))

        # 2. Normalización de Strings
        # medio_pago
        df_clean = df_clean.withColumn("medio_pago_clean", F.lower(F.trim(F.col("medio_pago"))))
        df_clean = df_clean.withColumn("medio_pago", 
            F.when(F.col("medio_pago").isNull(), "nequi")
             .when(F.col("medio_pago_clean").like("%transferencia%"), "transferencia") # Ojo: definicion decia 'transferencia' no 'transfer'
             .when(F.col("medio_pago_clean").like("%tarjeta%"), "tarjeta")
             .otherwise(F.col("medio_pago_clean"))
        ).drop("medio_pago_clean")

        # estado
        df_clean = df_clean.withColumn("estado_clean", F.lower(F.trim(F.col("estado"))))
        df_clean = df_clean.withColumn("estado", 
            F.when(F.col("estado").isNull(), "exitoso")
             .otherwise(F.col("estado_clean"))
        ).drop("estado_clean")
        
        # nombre_gasto (Trim + Lower)
        df_clean = df_clean.withColumn("nombre_gasto", F.lower(F.trim(F.col("nombre_gasto"))))

        # 3. Filtros Duros (Data Quality)
        # Rechazar gastos sin monto o sin proveedor
        df_clean = df_clean.filter(F.col("monto").isNotNull()) \
                           .filter(F.col("id_proveedor").isNotNull())

        # 4. Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_gastos"))

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
