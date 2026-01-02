
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
from jobs.utils.watermark import get_watermark, update_watermark

def run_silver_gastos():
    spark = get_spark_session("SilverGastos")
    try:
        # Path de entrada (Raw) y salida (Silver)
        output_path = f"{config.SILVER_PATH}/gastos"
        
        # 2. Leer datos Raw desde bucket (Parquet) y filtrar por watermark
        input_path = f"{config.RAW_PATH}/gastos"
        df_raw = spark.read.option("mergeSchema", "true").parquet(input_path)
        watermark = get_watermark(spark, "gastos")
        if watermark:
            df_raw = df_raw.filter(F.col("last_modified_at") > watermark)
        
        # 3. (Opcional) Filtrar solo el mes de prueba si TEST_MONTH está definido
        if config.TEST_MONTH:
            year, month = config.TEST_MONTH.split("-")
            df_raw = df_raw.filter((F.col("y") == int(year)) & (F.col("m") == month))
        
        print(f"📥 Lectura completada, filas: {df_raw.count()}")
        if df_raw.rdd.isEmpty(): return

        # Transformaciones y Reglas de Negocio (Según silver_gastos.sqlx)
        # ---------------------------------------------------------------
        
        # 1. Casting y Defaults (Adaptación Spark Robusta)
        def cast_to_timestamp(col_name):
            """Convierte columna a timestamp. Fechas vienen como STRING ISO desde Supabase."""
            return F.to_timestamp(F.col(col_name))

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
        
        # estado: Normalización robusta
        df_clean = df_clean.withColumn("estado_raw", F.lower(F.trim(F.col("estado"))))
        df_clean = df_clean.withColumn("estado", 
            F.when(F.col("estado_raw").isin(['pagado', 'exitoso', 'completado', 'aprobado']) | F.col("estado").isNull(), "pagado")
             .otherwise("pendiente")
        ).drop("estado_raw")
        

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
        # Primero eliminamos columnas de partición si existen
        cols_to_drop = [c for c in ["y", "m", "d"] if c in df_dedup.columns]
        if cols_to_drop:
            df_final = df_dedup.drop(*cols_to_drop)
        else:
            df_final = df_dedup
        
        df_final = df_final.withColumn("y", F.year("fecha_pago")) \
                           .withColumn("m", F.lpad(F.month("fecha_pago"), 2, "0")) \
                           .withColumn("d", F.lpad(F.dayofmonth("fecha_pago"), 2, "0")) \
                           .withColumn("d", F.lpad(F.dayofmonth("fecha_pago"), 2, "0"))

        # Escritura
        (df_final.write.mode("overwrite").partitionBy("y", "m", "d")
          .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        
        # Renombrar archivos al estándar
        rename_spark_output("silver", "gastos", output_path)

        # Update watermark disabled (Orchestrator managed)
        # max_ts = df_final.agg(F.max("last_modified_at")).collect()[0][0]
        # if max_ts:
        #     update_watermark(spark, max_ts, "gastos")

        print("✅ Gastos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_gastos()
