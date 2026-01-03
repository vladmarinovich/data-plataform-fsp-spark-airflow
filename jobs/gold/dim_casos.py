
"""
Job Gold: Dimensión Casos.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
import config
from jobs.utils.spark_session import get_spark_session
from jobs.utils.file_utils import rename_spark_output

def run_gold_dim_casos():
    spark = get_spark_session("GoldDimCasos")
    try:
        print("🚀 JOB GOLD: DIM CASOS")
        
        silver_casos = f"{config.SILVER_PATH}/casos"
        output_path = f"{config.GOLD_PATH}/dim_casos"
        
        df_casos = spark.read.parquet(silver_casos)
        
        # Particionado MENSUAL para el Lake (consistencia con Silver)
        df_final = df_casos.withColumn("es_caso_urgente", F.when(F.col("estado") == "urgente", True).otherwise(False)) \
                           .withColumn("dias_en_sistema", F.datediff(F.current_date(), F.col("fecha_ingreso"))) \
                           .withColumn("y", F.year("fecha_ingreso").cast("string")) \
                           .withColumn("m", F.lpad(F.month("fecha_ingreso"), 2, "0"))
                           
        # Escritura
        (df_final.write.mode("overwrite")
         .partitionBy("y", "m")
         .option("partitionOverwriteMode", "dynamic")
         .parquet(output_path))
        # rename_spark_output("gold", "dim_casos", output_path)
        
        print("✅ Gold Dim Casos procesada.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_dim_casos()
