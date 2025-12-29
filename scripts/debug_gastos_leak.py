"""
Script para diagnosticar la fuga de registros en FACT_GASTOS.
Meta: Encontrar los 17 registros perdidos (621 -> 604).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

def debug_gastos_leak():
    spark = get_spark_session("GastosLeakDebug")
    try:
        print("\n🕵️‍♂️ INVESTIGACIÓN FACT_GASTOS (621 vs 604)\n")
        
        # 1. RAW
        raw_path = f"{config.RAW_PATH}/gastos"
        df_raw = spark.read.parquet(raw_path)
        raw_count = df_raw.count()
        print(f"📦 RAW Gastos: {raw_count} filas")
        
        # 2. SILVER
        silver_path = f"{config.SILVER_PATH}/gastos"
        df_silver = spark.read.parquet(silver_path)
        silver_count = df_silver.count()
        print(f"🥈 SILVER Gastos: {silver_count} filas")
        
        # 3. Identificar los perdidos entre RAW y SILVER
        if raw_count > silver_count:
            print(f"\n🚫 SE PERDIERON {raw_count - silver_count} REGISTROS EN SILVER")
            
            # Aplicar filtros DQ uno por uno para ver cuál mata a los registros
            print("\n🔍 Analizando filtros DQ en raw_gastos:")
            
            null_monto = df_raw.filter(F.col("monto").isNull()).count()
            null_proveedor = df_raw.filter(F.col("id_proveedor").isNull()).count()
            
            print(f"   - Gastos con monto NULL: {null_monto}")
            print(f"   - Gastos con id_proveedor NULL: {null_proveedor}")
            
            # Verificar si hay duplicados por id_gasto en RAW
            dups = df_raw.groupBy("id_gasto").count().filter("count > 1").count()
            print(f"   - IDs duplicados en RAW: {dups}")
            
            # Ver registros que NO están en silver
            # Usamos left_anti join
            lost_in_silver = df_raw.join(df_silver, "id_gasto", "left_anti")
            print(f"\n📋 Muestra de los perdidos (Top 5):")
            lost_in_silver.select("id_gasto", "monto", "id_proveedor", "fecha_pago", "nombre_gasto").show(5)

        # 4. GOLD
        gold_path = f"{config.GOLD_PATH}/fact_gastos"
        df_gold = spark.read.parquet(gold_path)
        gold_count = df_gold.count()
        print(f"\n🏆 GOLD Fact Gastos: {gold_count} filas")

    finally:
        spark.stop()

if __name__ == "__main__":
    debug_gastos_leak()
