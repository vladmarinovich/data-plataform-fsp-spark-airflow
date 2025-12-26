
"""
Job Silver: Transformación y Limpieza de Donaciones.

Objetivo:
- Leer datos crudos desde RAW.
- Aplicar reglas de limpieza y calidad de datos.
- Deduplicar registros (CDC).
- Escribir tabla limpia en capa SILVER.

Ejecución:
$ spark-submit --master local[*] jobs/silver/donaciones.py
"""
import sys
from pathlib import Path

# Agregar raíz del proyecto al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

import config
from jobs.utils.spark_session import get_spark_session

def run_silver_donations():
    """Función principal del pipeline Silver de Donaciones."""
    
    # 1. Iniciar Spark Session con soporte GCS
    spark = get_spark_session("SilverDonaciones")
    
    try:
        print("="*80)
        print("🚀 INICIANDO JOB SILVER: DONACIONES")
        print("="*80)

        # Path de entrada (Raw) y salida (Silver)
        input_path = f"{config.RAW_PATH}/raw_donaciones"
        output_path = f"{config.SILVER_PATH}/donaciones"
        
        print(f"📥 Leyendo desde: {input_path}")
        
        # 2. Leer datos Raw (Schema evolution habilitado)
        # Spark infiere particiones anio/mes/dia automáticamente
        df_raw = spark.read.option("basePath", input_path).parquet(input_path + "/*")
        
        count_raw = df_raw.count()
        print(f"   📊 Registros Raw leídos: {count_raw}")
        
        if count_raw == 0:
            print("⚠️  No hay datos para procesar.")
            return

        # 3. Transformaciones y Limpieza (IMPLEMENTANDO LÓGICA DE NEGOCIO DEFINIDA)
        # -------------------------------------------------------------------------
        
        print("🛠️  Aplicando transformaciones de negocio...")
        
        # A. Normalización y Defaults (Según silver_donaciones.sqlx)
        # 1. Defaults FKs: id_donante y id_caso NULL => 541
        df_clean = df_raw.withColumn("id_donacion", F.col("id_donacion").cast("long")) \
                         .withColumn("id_donante", F.coalesce(F.col("id_donante").cast("long"), F.lit(541))) \
                         .withColumn("id_caso", F.coalesce(F.col("id_caso").cast("long"), F.lit(541))) \
                         .withColumn("monto", F.col("monto").cast("double")) \
                         .withColumn("fecha_donacion", F.col("fecha_donacion").cast("timestamp")) \
                         .withColumn("created_at_raw", F.col("created_at").cast("timestamp")) \
                         .withColumn("created_at", F.coalesce(F.col("created_at").cast("timestamp"), F.col("fecha_donacion").cast("timestamp"))) \
                         .withColumn("last_modified_at", F.coalesce(F.col("last_modified_at").cast("timestamp"), F.col("fecha_donacion").cast("timestamp")))

        # Fallback para fechas NULL aplicado arriba ↑

        # 2. Normalización de Strings (medio_pago, estado)
        # medio_pago: null -> nequi, 'transfer' -> transferencia, 'tarjeta' -> tarjeta
        df_clean = df_clean.withColumn("medio_pago_clean", F.lower(F.trim(F.col("medio_pago"))))
        
        df_clean = df_clean.withColumn("medio_pago", 
            F.when(F.col("medio_pago").isNull(), "nequi")
             .when(F.col("medio_pago_clean").like("%transfer%"), "transferencia")
             .when(F.col("medio_pago_clean").like("%tarjeta%"), "tarjeta")
             .otherwise(F.col("medio_pago_clean"))
        ).drop("medio_pago_clean")

        # estado: null -> exitoso
        df_clean = df_clean.withColumn("estado_clean", F.lower(F.trim(F.col("estado"))))
        df_clean = df_clean.withColumn("estado", 
            F.when(F.col("estado").isNull(), "exitoso")
             .otherwise(F.col("estado_clean"))
        ).drop("estado_clean")

        # 3. Columnas de Auditoría
        df_clean = df_clean.withColumn("fecha_ingesta", F.current_timestamp()) \
                           .withColumn("fuente", F.lit("raw_donaciones"))

        # C. Lógica de deduplicación (Snapshotting CDC)
        print("🔄 Deduplicando por ID y fecha de modificación...")
        
        window_spec = Window.partitionBy("id_donacion").orderBy(F.col("last_modified_at").desc())
        
        df_dedup = df_clean.withColumn("row_num", F.row_number().over(window_spec)) \
                           .filter(F.col("row_num") == 1) \
                           .drop("row_num")
        
        count_dedup = df_dedup.count()
        print(f"   ✅ Registros tras deduplicación: {count_dedup} (Descartados: {count_raw - count_dedup})")

        # D. Reglas de Negocio / Enriquecimiento
        # Asegurar que monto sea positivo
        df_final = df_dedup.filter(F.col("monto") >= 0)
        
        # Generar columnas de partición limpias (anio, mes) desde fecha_donacion
        # (A veces las particiones raw vienen como strings, mejor regenerarlas desde la fecha real)
        df_final = df_final.withColumn("anio_part", F.year("fecha_donacion")) \
                           .withColumn("mes_part", F.month("fecha_donacion"))

        # 4. Escritura en Capa Silver
        # -------------------------------------------------------------------------
        print(f"💾 Escribiendo a Silver: {output_path}")
        
        # Escribimos particionado por Anio/Mes (Granularidad mensual es suficiente para analítica histórica)
        # Mode: Overwrite (reemplaza las particiones afectadas, o Dynamic Partition Overwrite si se configura)
        # Para simplificar y garantizar consistencia, usamos overwrite a nivel partición
        
        (
            df_final.write
            .mode("overwrite")
            .partitionBy("anio_part", "mes_part")
            .format("parquet")
            .option("compression", "snappy")
            .option("partitionOverwriteMode", "dynamic") # Clave para no borrar todo el histórico, solo actualizar lo cambiado
            .save(output_path)
        )
        
        print("✅ Escritura completada exitosamente.")
        print("="*80)

    except Exception as e:
        print(f"❌ Error en job Silver Donaciones: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_donations()
