
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
        input_path = f"{config.RAW_PATH}/raw_hogar_de_paso"
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

        # Normalización
        df_clean = df_clean.withColumn("nombre_hogar", F.upper(F.trim(F.col("nombre_hogar"))))

        # Escritura (Full Overwrite sin partición)
        df_clean.write.mode("overwrite").parquet(output_path)
        
        rename_spark_output("silver", "hogar_de_paso", output_path)
        print("✅ Hogares de Paso procesados.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_hogar_de_paso()
