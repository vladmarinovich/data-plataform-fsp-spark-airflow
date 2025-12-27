
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
        input_path = f"{config.RAW_PATH}/raw_casos.parquet"
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

        # D. Calidad de Datos (DQ) y Cuarentena
        # --------------------------------------------------
        print("🛡️  Validando calidad de casos...")
        
        df_dq = df_dedup.withColumn("dq_errors", F.array())
        
        # Regla 1: IDs Obligatorios
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("id_caso").isNull(), 
                F.array_union(F.col("dq_errors"), F.array(F.lit("ID_CASO_NULO")))
            ).otherwise(F.col("dq_errors"))
        )
        
        # Regla 2: Fecha de Ingreso válida (2010 a hoy)
        df_dq = df_dq.withColumn("dq_errors", 
            F.when((F.col("fecha_ingreso") < F.lit("2010-01-01")) | (F.col("fecha_ingreso") > F.current_timestamp()),
                F.array_union(F.col("dq_errors"), F.array(F.lit("FECHA_INGRESO_INVALIDA")))
            ).otherwise(F.col("dq_errors"))
        )

        # Regla 3: Nombre obligatorio (no puede ser el default 'anonimo')
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("nombre_caso") == "anonimo",
                F.array_union(F.col("dq_errors"), F.array(F.lit("NOMBRE_CASO_NULO")))
            ).otherwise(F.col("dq_errors"))
        )

        # Dividimos: Silver vs Cuarentena
        df_final = df_dq.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        df_quarantine = df_dq.filter(F.size(F.col("dq_errors")) > 0)
        
        count_silver = df_final.count()
        count_dirty = df_quarantine.count()
        
        print(f"   ✨ Casos Válidos: {count_silver}")
        print(f"   ☣️  Casos en Cuarentena: {count_dirty}")

        # E. Escritura y Cuarentena
        if count_dirty > 0:
            quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/casos"
            print(f"☣️  Guardando casos dudosos en: {quarantine_path}")
            df_quarantine.write.mode("append").parquet(quarantine_path)
            df_quarantine.select("id_caso", "nombre_caso", "dq_errors").show(truncate=False)

        # Derivar particiones
        df_final = df_final.withColumn("y", F.year("fecha_ingreso")) \
                           .withColumn("m", F.lpad(F.month("fecha_ingreso"), 2, "0")) \
                           .withColumn("d", F.lpad(F.dayofmonth("fecha_ingreso"), 2, "0"))

        (df_final.write.mode("overwrite").partitionBy("y", "m", "d")
         .option("partitionOverwriteMode", "dynamic").parquet(output_path))
        
        # Renombrar archivos al estándar
        rename_spark_output("silver", "casos", output_path)
        
        print("✅ Casos procesados.")

    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_casos()
