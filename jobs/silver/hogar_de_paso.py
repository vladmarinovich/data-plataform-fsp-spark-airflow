
"""
Job Silver: Transformación Hogar de Paso.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_silver_hogar_de_paso():
    spark = get_spark_session("SilverHogar")
    try:
        print("🚀 JOB SILVER: HOGAR DE PASO")
        input_path = f"{config.RAW_PATH}/raw_hogar_de_paso.parquet"
        output_path = f"{config.SILVER_PATH}/hogar_de_paso"
        
        df_raw = spark.read.parquet(input_path)
        if df_raw.rdd.isEmpty(): return

        # Casting Robusto (Hogares a veces no tiene created_at en raw, lo derivemos si falta)
        def cast_to_timestamp(col_name):
            if col_name not in df_raw.columns: return F.current_timestamp()
            col = F.col(col_name)
            return F.when(
                col.cast("string").rlike(r'^\d+$'), 
                F.from_unixtime(col.cast("long")/1000000).cast("timestamp")
            ).otherwise(F.to_timestamp(col))

        df_clean = df_raw.withColumn("id_hogar_de_paso", F.col("id_hogar_de_paso").cast("long")) \
                         .withColumn("ultimo_contacto", cast_to_timestamp("ultimo_contacto"))
        
        # Auditoría / Default Date para partición
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp())

        # Normalización y Defaults (Basado en SQLX)
        df_clean = df_clean.withColumn("nombre_hogar", F.coalesce(F.upper(F.trim(F.col("nombre_hogar"))), F.lit("SIN NOMBRE"))) \
                           .withColumn("tipo_hogar", F.coalesce(F.trim(F.col("tipo_hogar")), F.lit("temporario"))) \
                           .withColumn("cupo_maximo", F.coalesce(F.col("capacidad_maxima").cast("int"), F.lit(0))) \
                           .withColumn("tarifa_diaria", F.coalesce(F.col("costo_mensual").cast("double")/30, F.lit(0.0)))

        # D. Calidad de Datos (DQ) y Cuarentena
        # --------------------------------------------------
        print("🛡️  Validando calidad de hogares...")
        df_dq = df_clean.withColumn("dq_errors", F.array())

        # Regla 1: Cupo no negativo
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("cupo_maximo") < 0, 
                F.array_union(F.col("dq_errors"), F.array(F.lit("CUPO_NEGATIVO")))
            ).otherwise(F.col("dq_errors"))
        )

        # Regla 2: Tarifa no negativa
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("tarifa_diaria") < 0, 
                F.array_union(F.col("dq_errors"), F.array(F.lit("TARIFA_NEGATIVA")))
            ).otherwise(F.col("dq_errors"))
        )

        # Regla 3: Nombre obligatorio
        df_dq = df_dq.withColumn("dq_errors", 
            F.when(F.col("nombre_hogar") == "SIN NOMBRE", 
                F.array_union(F.col("dq_errors"), F.array(F.lit("NOMBRE_HOGAR_NULO")))
            ).otherwise(F.col("dq_errors"))
        )

        # Separar Silver vs Cuarentena
        df_final = df_dq.filter(F.size(F.col("dq_errors")) == 0).drop("dq_errors")
        df_quarantine = df_dq.filter(F.size(F.col("dq_errors")) > 0)

        count_silver = df_final.count()
        count_dirty = df_quarantine.count()

        print(f"   ✨ Hogares Válidos: {count_silver}")
        print(f"   ☣️  Hogares en Cuarentena: {count_dirty}")

        # Escritura Cuarentena
        if count_dirty > 0:
            quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/hogar_de_paso"
            df_quarantine.write.mode("append").parquet(quarantine_path)

        # Escritura Silver (Master table sin partición)
        df_final.write.mode("overwrite").parquet(output_path)
        
        rename_spark_output("silver", "hogar_de_paso", output_path)
        print("✅ Hogares de Paso procesados.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_hogar_de_paso()
