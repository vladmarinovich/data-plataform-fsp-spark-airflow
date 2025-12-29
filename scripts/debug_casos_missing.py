"""
Script para investigar por qué faltan Casos.
Compara Raw vs Silver vs Quarantine.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

def investigate_missing_cases():
    spark = get_spark_session("DebugMissingCases")
    try:
        print("\n🕵️‍♂️ INVESTIGACIÓN DE CASOS PERDIDOS\n")
        
        # Paths
        raw_path = f"{config.RAW_PATH}/casos"
        silver_path = f"{config.SILVER_PATH}/casos"
        quarantine_path = f"{config.PROJECT_ROOT}/data/quarantine/casos"
        
        # 1. Análisis RAW
        print(f"1️⃣ RAW ({raw_path}):")
        if Path(raw_path).exists():
            df_raw = spark.read.parquet(raw_path)
            raw_count = df_raw.count()
            print(f"   Total Raw: {raw_count}")
            
            # Chequear nulos críticos
            null_dates = df_raw.filter(F.col("fecha_ingreso").isNull()).count()
            null_names = df_raw.filter(F.col("nombre_caso").isNull()).count()
            print(f"   ⚠️ Fecha Ingreso NULL: {null_dates}")
            print(f"   ⚠️ Nombre Caso NULL: {null_names}")
        else:
            print("   ❌ No existe RAW path")
            
        # 2. Análisis QUARANTINE
        print(f"\n2️⃣ CUARENTENA ({quarantine_path}):")
        if Path(quarantine_path).exists():
            try:
                df_quarantine = spark.read.parquet(quarantine_path)
                q_count = df_quarantine.count()
                print(f"   ☣️  Total en Cuarentena: {q_count}")
                if q_count > 0:
                    print("   Motivos principales:")
                    df_quarantine.select("id_caso", "nombre_caso", "dq_errors").show(10, truncate=False)
            except Exception:
                print("   (Carpeta existe pero quizás vacía o error lectura)")
        else:
            print("   ✅ Limpio (No existe carpeta Cuarentena)")

        # 3. Análisis SILVER
        print(f"\n3️⃣ SILVER ({silver_path}):")
        if Path(silver_path).exists():
            df_silver = spark.read.parquet(silver_path)
            silver_count = df_silver.count()
            print(f"   ✅ Total Silver: {silver_count}")
        else:
             print("   ❌ No existe SILVER path")

    finally:
        spark.stop()

if __name__ == "__main__":
    investigate_missing_cases()
