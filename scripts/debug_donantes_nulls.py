"""
Script para investigar NULOS en Silver Donantes que podrían estar rompiendo Gold.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

def debug_donantes():
    spark = get_spark_session("DebugDonantesNulls")
    try:
        print("\n🕵️‍♂️ DEBUG SILVER DONANTES\n")
        
        path = f"{config.SILVER_PATH}/donantes"
        df = spark.read.parquet(path)
        
        total = df.count()
        print(f"Total Filas: {total}")
        
        # Check Nulls in Key Columns
        df.select(
            F.count(F.when(F.col("created_at").isNull(), 1)).alias("created_at_NULL"),
            F.count(F.when(F.col("id_donante").isNull(), 1)).alias("id_donante_NULL")
        ).show()
        
        # Muestra de fechas
        df.select("id_donante", "created_at").show(5)

    finally:
        spark.stop()

if __name__ == "__main__":
    debug_donantes()
