
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
        
        df_clean = df_raw.withColumn("id_hogar_de_paso", F.col("id_hogar_de_paso").cast("long")) \
                         .withColumn("cupo_maximo", F.col("cupo_maximo").cast("int")) \
                         .withColumn("tarifa_diaria", F.col("tarifa_diaria").cast("double")) \
                         .withColumn("ultimo_contacto", F.col("ultimo_contacto").cast("timestamp"))
        
        df_clean = df_clean.dropDuplicates(["id_hogar_de_paso"])

        df_clean.write.mode("overwrite").parquet(output_path)
        print("✅ Hogar de Paso procesado.")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_hogar()
