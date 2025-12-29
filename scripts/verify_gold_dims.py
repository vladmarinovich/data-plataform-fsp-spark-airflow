"""
Script de Verificación de Dimensiones Gold.
Audita la calidad y existencia de las tablas maestras (DIM).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

DIMS_TO_CHECK = [
    "dim_calendario",
    "dim_donantes",
    "dim_proveedores",
    "dim_hogar_de_paso",
    "dim_casos",
    "fact_donaciones",
    "fact_gastos"
]

def verify_dims():
    spark = get_spark_session("VerifyGoldDims")
    try:
        print("\n🏆 VERIFICACIÓN DE DIMENSIONES (GOLD)\n")
        
        for dim in DIMS_TO_CHECK:
            path = f"{config.GOLD_PATH}/{dim}"
            print(f"🔍 Auditando: {dim.upper()} ...")
            
            if not Path(path.replace("file://", "")).exists():
                 print(f"   ❌ Error: Path no existe ({path})")
                 continue

            df = spark.read.parquet(path)
            count = df.count()
            
            print(f"   ✅ Rows: {count}")
            
            if count > 0:
                print("   📋 Muestra (Top 3):")
                # Mostrar columnas clave (primeras 5) para no saturar
                cols_to_show = df.columns[:5]
                df.select(*cols_to_show).show(3, truncate=False)
                
                # Chequeos específicos
                if dim == "dim_calendario":
                    min_date, max_date = df.select(F.min("fecha"), F.max("fecha")).first()
                    print(f"   📅 Rango: {min_date} a {max_date}")
            else:
                print("   ⚠️  ALERTA: Tabla vacía")
            
            print("-" * 60)

    except Exception as e:
        print(f"❌ Error verification: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_dims()
