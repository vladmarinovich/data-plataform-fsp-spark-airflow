"""
Script para contar Donantes en Raw y Silver.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.utils.spark_session import get_spark_session
import config

def count_donantes():
    spark = get_spark_session("DebugDonantes")
    try:
        print("\n🕵️‍♂️ CONTEO DE DONANTES\n")
        
        # 1. RAW
        raw_path = f"{config.RAW_PATH}/donantes"
        if Path(raw_path).exists():
            df_raw = spark.read.parquet(raw_path)
            print(f"   📦 RAW Donantes: {df_raw.count()}")
            # Ver rango de fechas si existe
            if "last_modified_at" in df_raw.columns:
                df_raw.selectExpr("min(last_modified_at)", "max(last_modified_at)").show(truncate=False)
        else:
            print(f"   ❌ RAW Path no existe: {raw_path}")

        # 2. SILVER
        silver_path = f"{config.SILVER_PATH}/donantes"
        if Path(silver_path).exists():
            df_silver = spark.read.parquet(silver_path)
            print(f"   🥈 SILVER Donantes: {df_silver.count()}")
        else:
            print(f"   ❌ SILVER Path no existe: {silver_path}")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    count_donantes()
