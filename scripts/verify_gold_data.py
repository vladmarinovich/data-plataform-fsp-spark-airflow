"""
Script de Verificación de Capa Gold.
Lee el Dashboard Financiero generado y muestra métricas clave.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

def verify_gold_results():
    spark = get_spark_session("VerifyGold")
    try:
        print("\n🏆 VERIFICACIÓN DE DASHBOARD FINANCIERO (GOLD)\n")
        
        # Path Local por defecto (o lo que diga config)
        path = f"{config.GOLD_PATH}/dashboard_financiero"
        
        if not Path(path.replace("file://", "")).exists():
             print(f"❌ Error: No se encuentra en {path}")
             return

        df = spark.read.parquet(path)
        
        print(f"Fuente: {path}")
        print(f"Total Meses Calculados: {df.count()}")
        
        print("\n📊 Últimos 6 meses de Balance Financiero:")
        df.select(
            "anio_mes", 
            F.col("total_donado").cast("int"), 
            F.col("total_gastado").cast("int"), 
            F.col("balance_neto").cast("int"),
            "estado_financiero_mes",
            F.round("runway_financiero", 2).alias("runway")
        ).orderBy(F.col("anio_mes").desc()).show(6, truncate=False)
        
        # Verificar integridad
        nulls = df.filter(F.col("balance_neto").isNull()).count()
        if nulls == 0:
            print("✅ Integridad de datos: OK (Sin nulos en balance)")
        else:
            print(f"⚠️ Alerta: {nulls} meses con balance nulo.")

    except Exception as e:
        print(f"❌ Error verification: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_gold_results()
