"""
Job Silver: Donaciones
Logica de negocio especifica para normalizar y limpiar donaciones.
Ref: silver_donaciones.sqlx
"""
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# Agregar path al sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from jobs.common import get_spark_session, read_raw, write_silver, standard_dedup

def transform_donaciones(df):
    """
    Aplica las reglas de negocio especificas de donaciones.
    """
    return df.select(
        # 1. IDs y Manejo de Nulos (Regla del 541)
        F.col("id_donacion").cast(IntegerType()),
        
        F.coalesce(F.col("id_donante").cast(IntegerType()), F.lit(541)).alias("id_donante"),
        F.coalesce(F.col("id_caso").cast(IntegerType()), F.lit(541)).alias("id_caso"),

        # 2. Fechas (Asegurar Timestamp)
        F.col("fecha_donacion").cast(TimestampType()),
        
        # 3. Monto y limpieza
        F.col("monto").cast(DoubleType()),
        
        # 4. Normalizacion de Medio de Pago
        F.when(F.col("medio_pago").isNull(), "nequi")
         .when(F.lower(F.col("medio_pago")).like("%transfer%"), "transferencia")
         .when(F.lower(F.col("medio_pago")).like("%tarjeta%"), "tarjeta")
         .otherwise(F.lower(F.trim(F.col("medio_pago")))).alias("medio_pago"),
         
        # 5. Estado
        F.when(F.col("estado").isNull(), "exitoso")
         .otherwise(F.lower(F.trim(F.col("estado")))).alias("estado"),

        # 6. Metadata original y calculada
        F.col("created_at").cast(TimestampType()),
        F.col("last_modified_at").cast(TimestampType()),
        F.current_timestamp().alias("fecha_ingesta"),
        F.lit("raw_donaciones").alias("fuente")
    ).filter(
        # Filtros de Calidad
        (F.col("id_donacion").isNotNull()) & 
        (F.col("monto") >= 0)
    ).withColumn(
        # Generar columnas de particion (Hive)
        "anio", F.year(F.col("fecha_donacion"))
    ).withColumn(
        "mes", F.lpad(F.month(F.col("fecha_donacion")), 2, "0")
    )

def main():
    spark = get_spark_session("Silver - Donaciones")
    spark.sparkContext.setLogLevel("WARN")
    
    TABLE = "donaciones"
    ID_COL = "id_donacion"
    DATE_COL = "last_modified_at"
    
    try:
        # 1. Leer Raw
        df_raw = read_raw(spark, TABLE)
        
        # 2. Transformaciones de Negocio (El corazón del Silver)
        print("⚙️ Aplicando reglas de negocio...")
        df_transformed = transform_donaciones(df_raw)
        
        # 3. Deduplicación (Usando las columnas ya limpias)
        df_final = standard_dedup(df_transformed, ID_COL, DATE_COL)
        
        # 4. Escribir Silver (Particionado)
        write_silver(df_final, TABLE, partition_cols=["anio", "mes"])
        
    except Exception as e:
        print(f"❌ Error en job {TABLE}: {e}")
        # Importante: Imprimir stacktrace para debug
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
