
"""Job Silver: Hogar de Paso."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session

def run_silver_hogar():
    spark = get_spark_session("SilverHogar")
    try:
        input_path = f"{config.RAW_PATH}/raw_hogar_de_paso"
        output_path = f"{config.SILVER_PATH}/hogar_de_paso"
        
        df_raw = spark.read.parquet(input_path + "/*.parquet")
        
        # Transformaciones de Negocio (silver_hogar_de_paso.sqlx)
        # -------------------------------------------------------
        
        df_clean = df_raw.withColumn("id_hogar_de_paso", F.col("id_hogar_de_paso").cast("long")) \
                         .withColumn("cupo_maximo", F.coalesce(F.col("cupo_maximo").cast("int"), F.lit(5))) \
                         .withColumn("tarifa_diaria", F.coalesce(F.col("tarifa_diaria").cast("double"), F.lit(20000.0))) \
                         .withColumn("desempeno", F.coalesce(F.col("desempeno").cast("double"), F.lit(3.3))) \
                         .withColumn("ultimo_contacto", F.coalesce(F.col("ultimo_contacto").cast("timestamp"), F.to_timestamp(F.lit("2024-01-01"))))

        # Normalización y Defaults Texto
        df_clean = df_clean.withColumn("nombre_hogar", F.trim(F.col("nombre_hogar"))) \
                           .withColumn("tipo_hogar", F.coalesce(F.lower(F.trim(F.col("tipo_hogar"))), F.lit("albergue"))) \
                           .withColumn("nombre_contacto", F.trim(F.col("nombre_contacto"))) \
                           .withColumn("correo", F.coalesce(F.lower(F.trim(F.col("correo"))), F.lit("desconocido"))) \
                           .withColumn("telefono", F.coalesce(F.trim(F.col("telefono")), F.lit("desconocido"))) \
                           .withColumn("ciudad", F.coalesce(F.lower(F.trim(F.col("ciudad"))), F.lit("bogota"))) \
                           .withColumn("pais", F.coalesce(F.initcap(F.trim(F.col("pais"))), F.lit("Colombia")))

        # Filtros Duros (Data Quality)
        df_clean = df_clean.filter(F.col("nombre_hogar").isNotNull()) \
                           .filter(F.col("nombre_contacto").isNotNull())
        
        # Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_hogar_de_paso"))

        df_clean.write.mode("overwrite").parquet(output_path)
        print("✅ Hogar de Paso procesado.")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_hogar()
