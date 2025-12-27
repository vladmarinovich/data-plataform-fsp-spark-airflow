
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
from jobs.utils.file_utils import rename_spark_output

def run_silver_gastos():
    spark = get_spark_session("SilverGastos")
    try:
        print("🚀 JOB SILVER: GASTOS")
        input_path = f"{config.RAW_PATH}/raw_gastos.parquet"
        output_path = f"{config.SILVER_PATH}/gastos"
        
        df_raw = spark.read.parquet(input_path)
        if df_raw.rdd.isEmpty(): return

        # Transformaciones y Reglas de Negocio (Según silver_gastos.sqlx)
        # ---------------------------------------------------------------
        
        # 1. Casting y Defaults (Adaptación Spark Robusta)
        def cast_to_timestamp(col_name):
            col = F.col(col_name)
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))

        df_clean = df_raw.withColumn("id_gasto", F.col("id_gasto").cast("long")) \
                         .withColumn("id_caso", F.coalesce(F.col("id_caso").cast("long"), F.lit(541))) \
                         .withColumn("id_proveedor", F.col("id_proveedor").cast("long")) \
                         .withColumn("monto", F.col("monto").cast("double"))
        
        # Procesar Fechas
        df_clean = df_clean.withColumn("fecha_pago", cast_to_timestamp("fecha_pago")) \
                           .withColumn("last_modified_at", cast_to_timestamp("last_modified_at")) \
                           .withColumn("created_at", cast_to_timestamp("created_at"))

        # 2. Normalización de Strings
        # medio_pago
        df_clean = df_clean.withColumn("medio_pago_clean", F.lower(F.trim(F.col("medio_pago"))))
        df_clean = df_clean.withColumn("medio_pago", 
            F.when(F.col("medio_pago").isNull(), "nequi")
             .when(F.col("medio_pago_clean").like("%transferencia%"), "transferencia") # Ojo: definicion decia 'transferencia' no 'transfer'
             .when(F.col("medio_pago_clean").like("%tarjeta%"), "tarjeta")
             .otherwise(F.col("medio_pago_clean"))
        ).drop("medio_pago_clean")
        

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
        df_final = df_dedup.withColumn("anio", F.year("fecha_pago")) \
                           .withColumn("mes", F.lpad(F.month("fecha_pago"), 2, "0")) \
                           .withColumn("dia", F.lpad(F.dayofmonth("fecha_pago"), 2, "0"))

        # Escritura
        (df_final.write.mode("overwrite").partitionBy("anio", "mes", "dia")
         .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        
        # Renombrar archivos al estándar
        rename_spark_output("silver", "gastos", output_path)
        
        print("✅ Gastos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_gastos()
