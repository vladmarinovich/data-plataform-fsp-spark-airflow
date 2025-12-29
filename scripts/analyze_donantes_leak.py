"""
Diagnóstico de pérdida de datos en el paso Silver -> Gold de Donantes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from jobs.utils.spark_session import get_spark_session
import config

def analyze_leak():
    spark = get_spark_session("LeakAnalysis")
    try:
        print("\n🔍 DIAGNÓSTICO DE PÉRDIDA DE DATOS: DIM_DONANTES\n")
        
        # 1. Lectura
        path_silver = f"{config.SILVER_PATH}/donantes"
        df_silver = spark.read.parquet(path_silver)
        count_silver = df_silver.count()
        print(f"1️⃣  SILVER Donantes: {count_silver} filas")
        
        # 2. Join con Calendario
        path_cal = f"{config.GOLD_PATH}/dim_calendario"
        df_cal = spark.read.parquet(path_cal)
        
        cal_cols = df_cal.select(
            F.col("fecha"),
            F.col("y").alias("anio_creacion")
        )
        
        # Simulamos el Join exacto del Job
        df_join = df_silver.join(cal_cols, F.to_date(df_silver.created_at) == cal_cols.fecha, "left")
        count_join = df_join.count()
        print(f"2️⃣  POST-JOIN Donantes: {count_join} filas")
        
        if count_join != count_silver:
            print(f"   ⚠️  Diferencia detectada en el JOIN: {count_silver} -> {count_join}")
            # El join de Spark puede duplicar si el calendario tiene la misma fecha duplicada 
            # o filtrar si no es un LEFT join (pero el código dice LEFT).
        
        # 3. Verificación de Escritura y Particionamiento
        df_final = df_join.withColumn("y_p", F.year("created_at")) \
                          .withColumn("m_p", F.month("created_at")) \
                          .withColumn("d_p", F.dayofmonth("created_at"))
        
        print(f"3️⃣  CONTEO FINAL EN MEMORIA: {df_final.count()} filas")
        
        # 4. Revisión de carpetas en disco tras el último run
        gold_path = Path(config.GOLD_PATH.replace("file://", "")) / "dim_donantes"
        if gold_path.exists():
            files = list(gold_path.glob("**/*.parquet"))
            print(f"4️⃣  INSPECCIÓN DISCO (Gold Path):")
            print(f"   Directorio: {gold_path}")
            print(f"   Número de archivos parquet encontrados: {len(files)}")
            
            # Si hay archivos, leamos EL DISCO directamente
            df_disk = spark.read.parquet(str(gold_path))
            print(f"   📊 TOTAL REAL LEÍDO DEL DISCO: {df_disk.count()} filas")
        else:
            print("   ❌ El directorio Gold no existe en disco.")

    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_leak()
